//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simd_runtime_proxy.h
//
// Identification: src/include/codegen/proxy/simd_runtime_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"

namespace peloton {
  namespace codegen {

    PROXY(SIMDRuntime) {
      DECLARE_METHOD(SIMDAdd4);
      DECLARE_METHOD(SIMDSub4);
      DECLARE_METHOD(SIMDCmpEq4);
      DECLARE_METHOD(SIMDCmpLt4);
      DECLARE_METHOD(SIMDCmpGt4);
      DECLARE_METHOD(SIMDFAdd4);
      DECLARE_METHOD(SIMDFSub4);
    };

  }
}