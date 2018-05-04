//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_scan_translator.cpp
//
// Identification: src/codegen/operator/table_scan_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/table_scan_translator.h"

#include "codegen/lang/if.h"
#include "codegen/proxy/executor_context_proxy.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/storage_manager_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
#include "codegen/proxy/zone_map_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/type.h"
#include "expression/comparison_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/operator_expression.h"
#include "expression/parameter_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/seq_scan_plan.h"
#include "storage/data_table.h"
#include "storage/zone_map_manager.h"

#include "llvm/IR/Module.h"

// #define ORIGINAL_ORDER

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// TABLE SCAN TRANSLATOR
//===----------------------------------------------------------------------===//

// Constructor
TableScanTranslator::TableScanTranslator(const planner::SeqScanPlan &scan,
                                         CompilationContext &context,
                                         Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      scan_(scan),
      table_(*scan_.GetTable()) {
  LOG_DEBUG("Constructing TableScanTranslator ...");

  // The restriction, if one exists
  const auto *predicate = GetScanPlan().GetPredicate();
  if (predicate != nullptr) {
    // If there is a predicate, prepare a translator for it
    context.Prepare(*predicate);

    // If the scan's predicate is SIMDable, install a boundary at the output
    if (predicate->IsSIMDable()) {
      pipeline.InstallBoundaryAtOutput(this);
    }
  }

  for (const auto &simd_predicate : GetScanPlan().GetSIMDPredicates()) {
    if (simd_predicate != nullptr) {
      context.Prepare(*simd_predicate);
    }
  }

  const auto *non_simd_predicate = GetScanPlan().GetNonSIMDPredicate();
  if (non_simd_predicate != nullptr) {
    context.Prepare(*non_simd_predicate);
  }

  LOG_TRACE("Finished constructing TableScanTranslator ...");
}

// Produce!
void TableScanTranslator::Produce() const {
  auto &codegen = GetCodeGen();
  auto &table = GetTable();

  LOG_TRACE("TableScan on [%u] starting to produce tuples ...", table.GetOid());
  codegen.Call(TransactionRuntimeProxy::ResetClockDuration, {});

  // Get the table instance from the database
  llvm::Value *storage_manager_ptr = GetStorageManagerPtr();
  llvm::Value *db_oid = codegen.Const32(table.GetDatabaseOid());
  llvm::Value *table_oid = codegen.Const32(table.GetOid());
  llvm::Value *table_ptr =
      codegen.Call(StorageManagerProxy::GetTableWithOid,
                   {storage_manager_ptr, db_oid, table_oid});

  // The selection vector for the scan
  auto *raw_vec = codegen.AllocateBuffer(
      codegen.Int32Type(), Vector::kDefaultVectorSize, "scanSelVector");
  Vector sel_vec{raw_vec, Vector::kDefaultVectorSize, codegen.Int32Type()};

  auto predicate = const_cast<expression::AbstractExpression *>(
      GetScanPlan().GetPredicate());
  llvm::Value *predicate_ptr = codegen->CreateIntToPtr(
      codegen.Const64((int64_t)predicate),
      AbstractExpressionProxy::GetType(codegen)->getPointerTo());
  size_t num_preds = 0;

  auto *zone_map_manager = storage::ZoneMapManager::GetInstance();
  if (predicate != nullptr && zone_map_manager->ZoneMapTableExists()) {
    if (predicate->IsZoneMappable()) {
      num_preds = predicate->GetNumberofParsedPredicates();
    }
  }
  ScanConsumer scan_consumer{*this, sel_vec};
  table_.GenerateScan(codegen, table_ptr, sel_vec.GetCapacity(), scan_consumer,
                      predicate_ptr, num_preds);
  codegen.Call(TransactionRuntimeProxy::PrintClockDuration, {});
  LOG_TRACE("TableScan on [%u] finished producing tuples ...", table.GetOid());
}

// Get the stringified name of this scan
std::string TableScanTranslator::GetName() const {
  std::string name = "Scan('" + GetTable().GetName() + "'";
  auto *predicate = GetScanPlan().GetPredicate();
  if (predicate != nullptr && predicate->IsSIMDable()) {
    name.append(", ").append(std::to_string(Vector::kDefaultVectorSize));
  }
  name.append(")");
  return name;
}

// Table accessor
const storage::DataTable &TableScanTranslator::GetTable() const {
  return *scan_.GetTable();
}

//===----------------------------------------------------------------------===//
// VECTORIZED SCAN CONSUMER
//===----------------------------------------------------------------------===//

// Constructor
TableScanTranslator::ScanConsumer::ScanConsumer(
    const TableScanTranslator &translator, Vector &selection_vector)
    : translator_(translator), selection_vector_(selection_vector) {}

// Generate the body of the vectorized scan
void TableScanTranslator::ScanConsumer::ProcessTuples(
    CodeGen &codegen, llvm::Value *tid_start, llvm::Value *tid_end,
    TileGroup::TileGroupAccess &tile_group_access) {
// TODO: Should visibility check be done here or in codegen::Table/TileGroup?

#ifdef ORIGINAL_ORDER
  // 1. Filter the rows in the range [tid_start, tid_end) by txn visibility
  FilterRowsByVisibility(codegen, tid_start, tid_end, selection_vector_);

  // 2. Filter rows by the given predicate (if one exists)
  auto *predicate = GetPredicate();
  if (predicate != nullptr) {
    // First perform a vectorized filter, putting TIDs into the selection vector
    FilterRowsByPredicate(codegen, tile_group_access, tid_start, tid_end,
                          selection_vector_);
  }
#else
  auto *predicate = GetPredicate();
  if (predicate != nullptr) {
    FilterRowsByPredicate(codegen, tile_group_access, tid_start, tid_end,
                          selection_vector_);
  } else {
    selection_vector_.SetNumElements(codegen.Const32(-1));
  }

  FilterRowsByVisibility(codegen, tid_start, tid_end, selection_vector_);
#endif

  // 3. Setup the (filtered) row batch and setup attribute accessors
  RowBatch batch{translator_.GetCompilationContext(),
                 tile_group_id_,
                 tid_start,
                 tid_end,
                 selection_vector_,
                 true};

  std::vector<TableScanTranslator::AttributeAccess> attribute_accesses;
  SetupRowBatch(batch, tile_group_access, attribute_accesses);

  // 4. Push the batch into the pipeline
  ConsumerContext context{translator_.GetCompilationContext(),
                          translator_.GetPipeline()};
  context.Consume(batch);
}

void TableScanTranslator::ScanConsumer::SetupRowBatch(
    RowBatch &batch, TileGroup::TileGroupAccess &tile_group_access,
    std::vector<TableScanTranslator::AttributeAccess> &access) const {
  // Grab a hold of the stuff we need (i.e., the plan, all the attributes, and
  // the IDs of the columns the scan _actually_ produces)
  const auto &scan_plan = translator_.GetScanPlan();
  std::vector<const planner::AttributeInfo *> ais;
  scan_plan.GetAttributes(ais);
  const auto &output_col_ids = scan_plan.GetColumnIds();

  // 1. Put all the attribute accessors into a vector
  access.clear();
  for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
    access.emplace_back(tile_group_access, ais[output_col_ids[col_idx]]);
  }

  // 2. Add the attribute accessors into the row batch
  for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
    auto *attribute = ais[output_col_ids[col_idx]];
    LOG_TRACE("Adding attribute '%s.%s' (%p) into row batch",
              scan_plan.GetTable()->GetName().c_str(), attribute->name.c_str(),
              attribute);
    batch.AddAttribute(attribute, &access[col_idx]);
  }
}

void TableScanTranslator::ScanConsumer::FilterRowsByVisibility(
    CodeGen &codegen, llvm::Value *tid_start, llvm::Value *tid_end,
    Vector &selection_vector) const {
  llvm::Value *executor_context_ptr =
      translator_.GetCompilationContext().GetExecutorContextPtr();
  llvm::Value *txn = codegen.Call(ExecutorContextProxy::GetTransaction,
                                  {executor_context_ptr});
  llvm::Value *raw_sel_vec = selection_vector.GetVectorPtr();

#ifdef ORIGINAL_ORDER
  // Invoke TransactionRuntime::PerformRead(...)
  llvm::Value *out_idx =
      codegen.Call(TransactionRuntimeProxy::PerformVectorizedRead,
                   {txn, tile_group_ptr_, tid_start, tid_end, raw_sel_vec,
                    codegen.Const32(-1)});
#else
  llvm::Value *out_idx =
      codegen.Call(TransactionRuntimeProxy::PerformVectorizedRead,
                   {txn, tile_group_ptr_, tid_start, tid_end, raw_sel_vec,
                    selection_vector.GetNumElements()});
#endif
  selection_vector.SetNumElements(out_idx);
}

// Get the predicate, if one exists
const expression::AbstractExpression *
TableScanTranslator::ScanConsumer::GetPredicate() const {
  return translator_.GetScanPlan().GetPredicate();
}

const std::vector<std::unique_ptr<expression::AbstractExpression>>
    &TableScanTranslator::ScanConsumer::GetSIMDPredicates() const {
  return translator_.GetScanPlan().GetSIMDPredicates();
}

const expression::AbstractExpression *
TableScanTranslator::ScanConsumer::GetNonSIMDPredicate() const {
  return translator_.GetScanPlan().GetNonSIMDPredicate();
}

static codegen::Value VectorizedDeriveValue(
    CodeGen &codegen, uint32_t N, RowBatch &batch, llvm::Value *start,
    bool filtered, const expression::AbstractExpression *exp,
    std::unordered_map<const planner::AttributeInfo *, codegen::Value> *cache = nullptr) {
  type::Type type{exp->GetValueType(), exp->IsNullable()};
  llvm::Type *llvm_type, *dummy;
  type.GetSqlType().GetTypeForMaterialization(codegen, llvm_type, dummy);

  if (auto *tve = dynamic_cast<const expression::TupleValueExpression *>(exp)) {
    auto *ai = tve->GetAttributeRef();

    if (cache != nullptr) {
      auto cache_iter = cache->find(ai);
      if (cache_iter != cache->end()) {
        return cache_iter->second;
      }
    }

    if (!filtered) {
      RowBatch::Row first_row = batch.GetRowAt(start);

      llvm::Value *ptr = first_row.DeriveFixedLengthPtrInTableScan(codegen, ai);
      ptr = codegen->CreateBitCast(
          ptr, llvm::VectorType::get(llvm_type, N)->getPointerTo());

      // llvm::Value *val = codegen->CreateMaskedLoad(ptr, 0,
      // llvm::Constant::getAllOnesValue(
      //     llvm::VectorType::get(codegen.BoolType(), N)));
      llvm::Value *val = codegen->CreateLoad(ptr);
      llvm::Value *is_null = nullptr;
      if (type.nullable) {
        auto &sql_type = type.GetSqlType();
        Value tmp_val{sql_type, val};
        Value null_val{sql_type,
                       codegen->CreateVectorSplat(
                           N, sql_type.GetNullValue(codegen).GetValue())};
        auto val_is_null = tmp_val.CompareEq(codegen, null_val);
        is_null = val_is_null.GetValue();
      }

      codegen::Value ret{type, val, nullptr, is_null};
      if (cache != nullptr) {
        cache->insert(std::make_pair(ai, ret));
      }
      return ret;
    } else {
      llvm::Value *val =
          llvm::UndefValue::get(llvm::VectorType::get(llvm_type, N));
      /* llvm::Value *is_null = type.nullable
                                 ? llvm::UndefValue::get(llvm::VectorType::get(
                                       codegen.BoolType(), N))
                                 : nullptr; */
      llvm::Value *is_null = nullptr;
      for (uint32_t i = 0; i < N; ++i) {
        RowBatch::Row row =
            batch.GetRowAt(codegen->CreateAdd(start, codegen.Const32(i)));
        codegen::Value eval_row = row.DeriveValue(codegen, ai);
        val = codegen->CreateInsertElement(val, eval_row.GetValue(), i);
        /* if (type.nullable) {
          is_null = codegen->CreateInsertElement(is_null,
                                                 eval_row.IsNull(codegen), i);
        } */
      }

      if (type.nullable) {
        auto &sql_type = type.GetSqlType();
        Value tmp_val{sql_type, val};
        Value null_val{sql_type, codegen->CreateVectorSplat(N, sql_type.GetNullValue(codegen).GetValue())};
        is_null = tmp_val.CompareEq(codegen, null_val).GetValue();
      }

      codegen::Value ret{type, val, nullptr, is_null};
      if (cache != nullptr) {
        cache->insert(std::make_pair(ai, ret));
      }
      return ret;
    }
  }

  if (dynamic_cast<const expression::ConstantValueExpression *>(exp) ||
      dynamic_cast<const expression::ParameterValueExpression *>(exp)) {
    RowBatch::Row first_row = batch.GetRowAt(start);

    Value const_val = first_row.DeriveValue(codegen, *exp);
    llvm::Value *ins_val = const_val.GetValue();
    llvm::Value *val = codegen->CreateVectorSplat(N, ins_val);
    llvm::Value *is_null = nullptr;
    if (type.nullable) {
      is_null = const_val.IsNull(codegen);
    }
    return Value{type, val, nullptr, is_null};
  }

  if (auto *comp_exp =
          dynamic_cast<const expression::ComparisonExpression *>(exp)) {
    Value lhs_val = VectorizedDeriveValue(codegen, N, batch, start, filtered,
                                          comp_exp->GetChild(0), cache);
    Value rhs_val = VectorizedDeriveValue(codegen, N, batch, start, filtered,
                                          comp_exp->GetChild(1), cache);

    Value comp_val;
    switch (comp_exp->GetExpressionType()) {
      case ExpressionType::COMPARE_EQUAL:
        comp_val = lhs_val.CompareEq(codegen, rhs_val);
        break;
      case ExpressionType::COMPARE_NOTEQUAL:
        comp_val = lhs_val.CompareNe(codegen, rhs_val);
        break;
      case ExpressionType::COMPARE_LESSTHAN:
        comp_val = lhs_val.CompareLt(codegen, rhs_val);
        break;
      case ExpressionType::COMPARE_LESSTHANOREQUALTO:
        comp_val = lhs_val.CompareLte(codegen, rhs_val);
        break;
      case ExpressionType::COMPARE_GREATERTHAN:
        comp_val = lhs_val.CompareGt(codegen, rhs_val);
        break;
      case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        comp_val = lhs_val.CompareGte(codegen, rhs_val);
        break;
      default:
        throw Exception{"Invalid expression type for vectorized translation " +
                        ExpressionTypeToString(comp_exp->GetExpressionType())};
    }

    return comp_val;
  }

  if (auto *op_exp =
          dynamic_cast<const expression::OperatorExpression *>(exp)) {
    Value lhs_val = VectorizedDeriveValue(codegen, N, batch, start, filtered,
                                          op_exp->GetChild(0), cache);
    Value rhs_val = VectorizedDeriveValue(codegen, N, batch, start, filtered,
                                          op_exp->GetChild(1), cache);

    Value op_val;
    switch (op_exp->GetExpressionType()) {
      case ExpressionType::OPERATOR_PLUS:
        op_val = lhs_val.Add(codegen, rhs_val);
        break;
      case ExpressionType::OPERATOR_MINUS:
        op_val = lhs_val.Sub(codegen, rhs_val);
        break;
      case ExpressionType::OPERATOR_MULTIPLY:
        op_val = lhs_val.Mul(codegen, rhs_val);
        break;
      default:
        throw Exception{"Invalid expression type for vectorized translation " +
                        ExpressionTypeToString(op_exp->GetExpressionType())};
    }

    return op_val;
  }

  throw Exception{"Invalid expression type for vectorized translation " +
                  ExpressionTypeToString(exp->GetExpressionType())};
}

void TableScanTranslator::ScanConsumer::FilterRowsByPredicate(
    CodeGen &codegen, const TileGroup::TileGroupAccess &access,
    llvm::Value *tid_start, llvm::Value *tid_end,
    Vector &selection_vector) const {
  auto &compilation_ctx = translator_.GetCompilationContext();

  const auto &simd_predicates = GetSIMDPredicates();
  const auto *non_simd_predicate = GetNonSIMDPredicate();

  codegen.Call(TransactionRuntimeProxy::GetClockStart, {});

  if (simd_predicates.empty() && non_simd_predicate == nullptr) {
    non_simd_predicate = GetPredicate();
  }

  uint32_t N = 32;

#ifdef ORIGINAL_ORDER
  for (auto &simd_predicate : simd_predicates) {
    LOG_INFO("SIMD predicate detected");
    LOG_INFO("%s", simd_predicate->GetInfo().c_str());

    // The batch we're filtering
    RowBatch batch{compilation_ctx, tile_group_id_,   tid_start,
                   tid_end,         selection_vector, true};

    // Determine the attributes the predicate needs
    std::unordered_set<const planner::AttributeInfo *> used_attributes;
    simd_predicate->GetUsedAttributes(used_attributes);

    // Setup the row batch with attribute accessors for the predicate
    std::vector<AttributeAccess> attribute_accessors;
    for (const auto *ai : used_attributes) {
      attribute_accessors.emplace_back(access, ai);
    }
    for (uint32_t i = 0; i < attribute_accessors.size(); i++) {
      auto &accessor = attribute_accessors[i];
      batch.AddAttribute(accessor.GetAttributeRef(), &accessor);
    }

    auto *orig_size = batch.GetNumValidRows(codegen);
    auto *align_size = codegen->CreateMul(
        codegen.Const32(N), codegen->CreateUDiv(orig_size, codegen.Const32(N)));
    selection_vector.SetNumElements(align_size);

    batch.VectorizedIterate(codegen, N, [&](RowBatch::
                                                VectorizedIterateCallback::
                                                    IterationInstance &ins) {
      auto comp_val = VectorizedDeriveValue(codegen, N, batch, ins.start, true,
                                            simd_predicate.get());

      PELOTON_ASSERT(comp_val.GetType().GetSqlType() ==
                     type::Boolean::Instance());
      llvm::Value *bool_val =
          type::Boolean::Instance().Reify(codegen, comp_val);

      llvm::Value *final_pos = ins.write_pos;

      for (uint32_t i = 0; i < N; ++i) {
        RowBatch::OutputTracker tracker{batch.GetSelectionVector(), final_pos};
        RowBatch::Row row = batch.GetRowAt(
            codegen->CreateAdd(ins.start, codegen.Const32(i)), &tracker);
        row.SetValidity(codegen, codegen->CreateExtractElement(bool_val, i));
        final_pos = tracker.GetFinalOutputPos();
      }

      return final_pos;
    });

    auto pre_bb = codegen->GetInsertBlock();
    auto *check_post_batch_bb = llvm::BasicBlock::Create(
        codegen.GetContext(), "checkPostBatch", pre_bb->getParent());
    auto *loop_post_batch_bb = llvm::BasicBlock::Create(
        codegen.GetContext(), "loopPostBatch", pre_bb->getParent());
    auto *end_post_batch_bb = llvm::BasicBlock::Create(
        codegen.GetContext(), "endPostBatch", pre_bb->getParent());

    codegen->CreateBr(check_post_batch_bb);

    codegen->SetInsertPoint(check_post_batch_bb);
    auto *idx_cur = codegen->CreatePHI(align_size->getType(), 2);
    idx_cur->addIncoming(align_size, pre_bb);
    auto *write_pos =
        codegen->CreatePHI(selection_vector.GetNumElements()->getType(), 2);
    write_pos->addIncoming(selection_vector.GetNumElements(), pre_bb);
    auto *cond = codegen->CreateICmpULT(idx_cur, orig_size);
    codegen->CreateCondBr(cond, loop_post_batch_bb, end_post_batch_bb);

    codegen->SetInsertPoint(loop_post_batch_bb);
    {
      RowBatch::OutputTracker tracker{batch.GetSelectionVector(), write_pos};
      RowBatch::Row row = batch.GetRowAt(idx_cur, &tracker);

      codegen::Value valid_row = row.DeriveValue(codegen, *simd_predicate);

      // Reify the boolean value since it may be NULL
      PELOTON_ASSERT(valid_row.GetType().GetSqlType() ==
                     type::Boolean::Instance());
      llvm::Value *bool_val =
          type::Boolean::Instance().Reify(codegen, valid_row);

      row.SetValidity(codegen, bool_val);
      idx_cur->addIncoming(codegen->CreateAdd(idx_cur, codegen.Const32(1)),
                           codegen->GetInsertBlock());
      write_pos->addIncoming(tracker.GetFinalOutputPos(),
                             codegen->GetInsertBlock());
      codegen->CreateBr(check_post_batch_bb);
    }

    codegen->SetInsertPoint(end_post_batch_bb);
    batch.UpdateWritePosition(write_pos);
  }
#else
  llvm::Value *align_start = tid_start;
  llvm::Value *orig_size = codegen->CreateSub(tid_end, tid_start);
  llvm::Value *align_size = codegen->CreateMul(
      codegen.Const32(N), codegen->CreateUDiv(orig_size, codegen.Const32(N)));
  llvm::Value *align_end = codegen->CreateAdd(tid_start, align_size);

  // The batch we're filtering
  RowBatch batch{compilation_ctx, tile_group_id_,   align_start,
                 align_end,       selection_vector, false};

  // Determine the attributes the predicate needs
  std::unordered_set<const planner::AttributeInfo *> used_attributes;
  GetPredicate()->GetUsedAttributes(used_attributes);

  // Setup the row batch with attribute accessors for the predicate
  std::vector<AttributeAccess> attribute_accessors;
  for (const auto *ai : used_attributes) {
    attribute_accessors.emplace_back(access, ai);
  }
  for (uint32_t i = 0; i < attribute_accessors.size(); i++) {
    auto &accessor = attribute_accessors[i];
    batch.AddAttribute(accessor.GetAttributeRef(), &accessor);
  }

  std::unordered_map<const planner::AttributeInfo *, codegen::Value> cache;

  batch.VectorizedIterate(codegen, N, [&](RowBatch::VectorizedIterateCallback::
                                              IterationInstance &ins) {
    llvm::Value *mask = llvm::Constant::getAllOnesValue(
        llvm::VectorType::get(codegen.BoolType(), N));

    for (auto &simd_predicate : simd_predicates) {
      LOG_INFO("SIMD predicate detected");
      LOG_INFO("%s", simd_predicate->GetInfo().c_str());

      auto comp_val = VectorizedDeriveValue(codegen, N, batch, ins.start, false,
                                            simd_predicate.get(), &cache);

      PELOTON_ASSERT(comp_val.GetType().GetSqlType() ==
                     type::Boolean::Instance());
      auto bool_val = type::Boolean::Instance().Reify(codegen, comp_val);

      mask = codegen->CreateAnd(mask, bool_val);
    }

    llvm::Value *final_pos = ins.write_pos;

    for (uint32_t i = 0; i < N; ++i) {
      RowBatch::OutputTracker tracker{batch.GetSelectionVector(), final_pos};
      RowBatch::Row row = batch.GetRowAt(
          codegen->CreateAdd(ins.start, codegen.Const32(i)), &tracker);
      row.SetValidity(codegen, codegen->CreateExtractElement(mask, i));
      final_pos = tracker.GetFinalOutputPos();
    }

    return final_pos;
  });

  batch.SetFiltered(false);

  auto pre_bb = codegen->GetInsertBlock();
  auto *check_post_batch_bb = llvm::BasicBlock::Create(
      codegen.GetContext(), "checkPostBatch", pre_bb->getParent());
  auto *loop_post_batch_bb = llvm::BasicBlock::Create(
      codegen.GetContext(), "loopPostBatch", pre_bb->getParent());
  auto *end_post_batch_bb = llvm::BasicBlock::Create(
      codegen.GetContext(), "endPostBatch", pre_bb->getParent());

  codegen->CreateBr(check_post_batch_bb);

  codegen->SetInsertPoint(check_post_batch_bb);
  auto *idx_cur = codegen->CreatePHI(align_size->getType(), 2);
  idx_cur->addIncoming(align_size, pre_bb);
  auto *write_pos =
      codegen->CreatePHI(selection_vector.GetNumElements()->getType(), 2);
  write_pos->addIncoming(selection_vector.GetNumElements(), pre_bb);
  auto *cond = codegen->CreateICmpULT(idx_cur, orig_size);
  codegen->CreateCondBr(cond, loop_post_batch_bb, end_post_batch_bb);

  codegen->SetInsertPoint(loop_post_batch_bb);
  {
    RowBatch::OutputTracker tracker{batch.GetSelectionVector(), write_pos};
    RowBatch::Row row = batch.GetRowAt(idx_cur, &tracker);
    llvm::Value *mask = codegen.ConstBool(true);

    for (auto &simd_predicate : simd_predicates) {
      codegen::Value valid_row = row.DeriveValue(codegen, *simd_predicate);
      PELOTON_ASSERT(valid_row.GetType().GetSqlType() ==
                     type::Boolean::Instance());
      llvm::Value *bool_val =
          type::Boolean::Instance().Reify(codegen, valid_row);
      mask = codegen->CreateAnd(mask, bool_val);
    }

    row.SetValidity(codegen, mask);
    idx_cur->addIncoming(codegen->CreateAdd(idx_cur, codegen.Const32(1)),
                         codegen->GetInsertBlock());
    write_pos->addIncoming(tracker.GetFinalOutputPos(),
                           codegen->GetInsertBlock());
    codegen->CreateBr(check_post_batch_bb);
  }

  codegen->SetInsertPoint(end_post_batch_bb);
  batch.UpdateWritePosition(write_pos);
#endif

  if (non_simd_predicate != nullptr) {
    // The batch we're filtering
    RowBatch batch{compilation_ctx, tile_group_id_,   tid_start,
                   tid_end,         selection_vector, true};

    // Determine the attributes the predicate needs
    std::unordered_set<const planner::AttributeInfo *> used_attributes;
    non_simd_predicate->GetUsedAttributes(used_attributes);

    // Setup the row batch with attribute accessors for the predicate
    std::vector<AttributeAccess> attribute_accessors;
    for (const auto *ai : used_attributes) {
      attribute_accessors.emplace_back(access, ai);
    }
    for (uint32_t i = 0; i < attribute_accessors.size(); i++) {
      auto &accessor = attribute_accessors[i];
      batch.AddAttribute(accessor.GetAttributeRef(), &accessor);
    }

    // Iterate over the batch using a scalar loop
    batch.Iterate(codegen, [&](RowBatch::Row &row) {
      // Evaluate the predicate to determine row validity
      codegen::Value valid_row = row.DeriveValue(codegen, *non_simd_predicate);

      // Reify the boolean value since it may be NULL
      PELOTON_ASSERT(valid_row.GetType().GetSqlType() ==
                     type::Boolean::Instance());
      llvm::Value *bool_val =
          type::Boolean::Instance().Reify(codegen, valid_row);

      // Set the validity of the row
      row.SetValidity(codegen, bool_val);
    });
  }

  codegen.Call(TransactionRuntimeProxy::GetClockPause, {});
}

//===----------------------------------------------------------------------===//
// ATTRIBUTE ACCESS
//===----------------------------------------------------------------------===//

TableScanTranslator::AttributeAccess::AttributeAccess(
    const TileGroup::TileGroupAccess &access, const planner::AttributeInfo *ai)
    : tile_group_access_(access), ai_(ai) {}

codegen::Value TableScanTranslator::AttributeAccess::Access(
    CodeGen &codegen, RowBatch::Row &row) {
  auto raw_row = tile_group_access_.GetRow(row.GetTID(codegen));
  return raw_row.LoadColumn(codegen, ai_->attribute_id);
}

llvm::Value *TableScanTranslator::AttributeAccess::GetFixedLengthPtr(
    peloton::codegen::CodeGen &codegen, peloton::codegen::RowBatch::Row &row) {
  auto raw_row = tile_group_access_.GetRow(row.GetTID(codegen));
  return raw_row.GetFixedLengthColumnPtr(codegen, ai_->attribute_id);
}

}  // namespace codegen
}  // namespace peloton
