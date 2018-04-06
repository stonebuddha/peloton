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
      DECLARE_METHOD(SIMDAdd);
      DECLARE_METHOD(SIMDCmpEq);
      DECLARE_METHOD(SIMDFAdd);
    };

  }
}