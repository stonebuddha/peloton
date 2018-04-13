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
#include "codegen/proxy/storage_manager_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/zone_map_proxy.h"
#include "codegen/type/boolean_type.h"
#include "codegen/type/type.h"
#include "expression/comparison_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/seq_scan_plan.h"
#include "storage/data_table.h"
#include "storage/zone_map_manager.h"

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
  LOG_DEBUG("Finished constructing TableScanTranslator ...");
}

// Produce!
void TableScanTranslator::Produce() const {
  auto &codegen = GetCodeGen();
  auto &table = GetTable();

  LOG_TRACE("TableScan on [%u] starting to produce tuples ...", table.GetOid());

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

  // 1. Filter the rows in the range [tid_start, tid_end) by txn visibility
  FilterRowsByVisibility(codegen, tid_start, tid_end, selection_vector_);

  // 2. Filter rows by the given predicate (if one exists)
  auto *predicate = GetPredicate();
  if (predicate != nullptr) {
    // First perform a vectorized filter, putting TIDs into the selection vector
    FilterRowsByPredicate(codegen, tile_group_access, tid_start, tid_end,
                          selection_vector_);
  }

  // 3. Setup the (filtered) row batch and setup attribute accessors
  RowBatch batch{translator_.GetCompilationContext(), tile_group_id_, tid_start,
                 tid_end, selection_vector_, true};

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

  // Invoke TransactionRuntime::PerformRead(...)
  llvm::Value *out_idx =
      codegen.Call(TransactionRuntimeProxy::PerformVectorizedRead,
                   {txn, tile_group_ptr_, tid_start, tid_end, raw_sel_vec});
  selection_vector.SetNumElements(out_idx);
}

// Get the predicate, if one exists
const expression::AbstractExpression *
TableScanTranslator::ScanConsumer::GetPredicate() const {
  return translator_.GetScanPlan().GetPredicate();
}

const std::vector<std::unique_ptr<expression::AbstractExpression>>& TableScanTranslator::ScanConsumer::GetSIMDPredicates() const {
  return translator_.GetScanPlan().GetSIMDPredicates();
}

const expression::AbstractExpression* TableScanTranslator::ScanConsumer::GetNonSIMDPredicate() const {
  return translator_.GetScanPlan().GetNonSIMDPredicate();
}

void TableScanTranslator::ScanConsumer::FilterRowsByPredicate(
    CodeGen &codegen, const TileGroup::TileGroupAccess &access,
    llvm::Value *tid_start, llvm::Value *tid_end,
    Vector &selection_vector) const {
  // The batch we're filtering
  auto &compilation_ctx = translator_.GetCompilationContext();
  RowBatch batch{compilation_ctx, tile_group_id_,   tid_start,
                 tid_end,         selection_vector, true};

  const auto &simd_predicates = GetSIMDPredicates();
  const auto *non_simd_predicate = GetNonSIMDPredicate();

  if (simd_predicates.empty() && non_simd_predicate == nullptr) {
    non_simd_predicate = GetPredicate();
  }

  for (auto &simd_predicate : simd_predicates) {
    // Determine the attributes the predicate needs
    std::unordered_set<const planner::AttributeInfo *> used_attributes;
    simd_predicate->GetUsedAttributes(used_attributes);
    LOG_DEBUG("SIMD predicate detected");

    // Setup the row batch with attribute accessors for the predicate
    std::vector<AttributeAccess> attribute_accessors;
    for (const auto *ai : used_attributes) {
      attribute_accessors.emplace_back(access, ai);
    }
    for (uint32_t i = 0; i < attribute_accessors.size(); i++) {
      auto &accessor = attribute_accessors[i];
      batch.AddAttribute(accessor.GetAttributeRef(), &accessor);
    }

    uint32_t N = 32;

    auto *orig_size = selection_vector.GetNumElements();
    auto *align_size = codegen->CreateMul(codegen.Const32(N), codegen->CreateUDiv(orig_size, codegen.Const32(N)));
    selection_vector.SetNumElements(align_size);

    batch.VectorizedIterate(codegen, N, [&](RowBatch::VectorizedIterateCallback::IterationInstance &ins) {
      llvm::Value *final_pos = ins.write_pos;

      llvm::Value *lhs = nullptr;
      llvm::Value *rhs = nullptr;

      auto *lch = simd_predicate->GetChild(0);
      auto *rch = simd_predicate->GetChild(1);

      auto typ_lch = type::Type(lch->GetValueType(), false);
      auto typ_rch = type::Type(rch->GetValueType(), false);

      auto cast_lch = typ_lch;
      auto cast_rch = typ_rch;
      type::TypeSystem::GetComparison(typ_lch, cast_lch, typ_rch, cast_rch);

      llvm::Type *dummy, *typ_lhs, *typ_rhs;
      cast_lch.GetSqlType().GetTypeForMaterialization(codegen, typ_lhs, dummy);
      cast_rch.GetSqlType().GetTypeForMaterialization(codegen, typ_rhs, dummy);

      lhs = llvm::UndefValue::get(llvm::VectorType::get(typ_lhs, N));
      rhs = llvm::UndefValue::get(llvm::VectorType::get(typ_rhs, N));
      for (uint32_t i = 0; i < N; ++i) {
        RowBatch::Row row = batch.GetRowAt(codegen->CreateAdd(ins.start, codegen.Const32(i)));
        codegen::Value eval_row = row.DeriveValue(codegen, *lch);
        llvm::Value *ins_val = eval_row.CastTo(codegen, cast_lch).GetValue();
        lhs = codegen->CreateInsertElement(lhs, ins_val, i);
      }
      for (uint32_t i = 0; i < N; ++i) {
        RowBatch::Row row = batch.GetRowAt(codegen->CreateAdd(ins.start, codegen.Const32(i)));
        codegen::Value eval_row = row.DeriveValue(codegen, *rch);
        llvm::Value *ins_val = eval_row.CastTo(codegen, cast_rch).GetValue();
        rhs = codegen->CreateInsertElement(rhs, ins_val, i);
      }
      codegen::Value val_lhs(cast_lch, lhs);
      codegen::Value val_rhs(cast_rch, rhs);

      auto *cmp_exp = static_cast<const expression::ComparisonExpression *>(simd_predicate.get());
      llvm::Value *comp = nullptr;
      switch (cmp_exp->GetExpressionType()) {
        case ExpressionType::COMPARE_EQUAL:
          comp = val_lhs.CompareEq(codegen, val_rhs).GetValue();
          break;
        case ExpressionType::COMPARE_NOTEQUAL:
          comp = val_lhs.CompareNe(codegen, val_rhs).GetValue();
          break;
        case ExpressionType::COMPARE_LESSTHAN:
          comp = val_lhs.CompareLt(codegen, val_rhs).GetValue();
          break;
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
          comp = val_lhs.CompareLte(codegen, val_rhs).GetValue();
          break;
        case ExpressionType::COMPARE_GREATERTHAN:
          comp = val_lhs.CompareGt(codegen, val_rhs).GetValue();
          break;
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
          comp = val_lhs.CompareGte(codegen, val_rhs).GetValue();
          break;
        default:;
      }

      for (uint32_t i = 0; i < N; ++i) {
        RowBatch::OutputTracker tracker{batch.GetSelectionVector(), final_pos};
        RowBatch::Row row = batch.GetRowAt(codegen->CreateAdd(ins.start, codegen.Const32(i)), &tracker);
        row.SetValidity(codegen, codegen->CreateExtractElement(comp, i));
        final_pos = tracker.GetFinalOutputPos();
      }

      return final_pos;
    });

    auto pre_bb = codegen->GetInsertBlock();
    auto *check_post_batch_bb = llvm::BasicBlock::Create(codegen.GetContext(), "checkPostBatch", pre_bb->getParent());
    auto *loop_post_batch_bb = llvm::BasicBlock::Create(codegen.GetContext(), "loopPostBatch", pre_bb->getParent());
    auto *end_post_batch_bb = llvm::BasicBlock::Create(codegen.GetContext(), "endPostBatch", pre_bb->getParent());

    codegen->CreateBr(check_post_batch_bb);

    codegen->SetInsertPoint(check_post_batch_bb);
    auto *idx_cur = codegen->CreatePHI(align_size->getType(), 2);
    idx_cur->addIncoming(align_size, pre_bb);
    auto *write_pos = codegen->CreatePHI(selection_vector.GetNumElements()->getType(), 2);
    write_pos->addIncoming(selection_vector.GetNumElements(), pre_bb);
    auto *cond = codegen->CreateICmpULT(idx_cur, orig_size);
    codegen->CreateCondBr(cond, loop_post_batch_bb, end_post_batch_bb);

    codegen->SetInsertPoint(loop_post_batch_bb);
    {
      RowBatch::OutputTracker tracker{batch.GetSelectionVector(), write_pos};
      RowBatch::Row row = batch.GetRowAt(idx_cur, &tracker);
      codegen::Value valid_row = row.DeriveValue(codegen, *simd_predicate);
      PELOTON_ASSERT(valid_row.GetType().GetSqlType() == type::Boolean::Instance());
      llvm::Value *bool_val = type::Boolean::Instance().Reify(codegen, valid_row);
      row.SetValidity(codegen, bool_val);
      idx_cur->addIncoming(codegen->CreateAdd(idx_cur, codegen.Const32(1)), loop_post_batch_bb);
      write_pos->addIncoming(tracker.GetFinalOutputPos(), loop_post_batch_bb);
      codegen->CreateBr(check_post_batch_bb);
    }

    codegen->SetInsertPoint(end_post_batch_bb);
    batch.UpdateWritePosition(write_pos);
  }

  if (non_simd_predicate != nullptr) {
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
      PELOTON_ASSERT(valid_row.GetType().GetSqlType() == type::Boolean::Instance());
      llvm::Value *bool_val = type::Boolean::Instance().Reify(codegen, valid_row);

      // Set the validity of the row
      row.SetValidity(codegen, bool_val);
    });
  }
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

}  // namespace codegen
}  // namespace peloton
