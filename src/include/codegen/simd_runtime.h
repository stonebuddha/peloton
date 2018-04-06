//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simd_runtime.h
//
// Identification: src/include/codegen/simd_runtime.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
  namespace codegen {

    class SIMDRuntime {
    public:
      static void SIMDAdd(int a[4], int b[4], int c[4]);
      static void SIMDCmpEq(int a[4], int b[4], int c[4]);
      static void SIMDFAdd(float a[4], float b[4], float c[4]);
    };

  }
}