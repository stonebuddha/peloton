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
      DECLARE_METHOD(SIMDI32Add4);
      DECLARE_METHOD(SIMDI32Sub4);
      DECLARE_METHOD(SIMDI32CmpEq4);
      DECLARE_METHOD(SIMDI32CmpLt4);
      DECLARE_METHOD(SIMDI32CmpGt4);

      DECLARE_METHOD(SIMDI16CmpEq8);

      DECLARE_METHOD(SIMDI8CmpEq16);

      DECLARE_METHOD(SIMDFAdd4);
      DECLARE_METHOD(SIMDFSub4);
    };

  }
}