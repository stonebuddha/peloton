//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simd_runtime.cpp
//
// Identification: src/codegen/simd_runtime.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <x86intrin.h>

#include "codegen/simd_runtime.h"

namespace peloton {
  namespace codegen {

    void SIMDRuntime::SIMDAdd(int a[4], int b[4], int c[4]) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_add_epi32(as, bs);
      _mm_store_si128((__m128i *) c, cs);
    }

    void SIMDRuntime::SIMDCmpEq(int a[4], int b[4], int c[4]) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_cmpeq_epi32(as, bs);
      _mm_store_si128((__m128i *) c, cs);
    }

    void SIMDRuntime::SIMDFAdd(float a[4], float b[4], float c[4]) {
      __m128 as = _mm_load_ps(a);
      __m128 bs = _mm_load_ps(b);
      __m128 cs = _mm_add_ps(as, bs);
      _mm_store_ps(c, cs);
    }

  }
}