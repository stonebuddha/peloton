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

    void SIMDRuntime::SIMDAdd4(int32_t *a, int32_t *b, int32_t *c) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_add_epi32(as, bs);
      _mm_store_si128((__m128i *) c, cs);
    }

    void SIMDRuntime::SIMDSub4(int32_t *a, int32_t *b, int32_t *c) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_sub_epi32(as, bs);
      _mm_store_si128((__m128i *) c, cs);
    }

    void SIMDRuntime::SIMDCmpEq4(int32_t *a, int32_t *b, int32_t *c) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_cmpeq_epi32(as, bs);
      _mm_store_si128((__m128i *) c, cs);
    }

    void SIMDRuntime::SIMDCmpLt4(int32_t *a, int32_t *b, int32_t *c) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_cmplt_epi32(as, bs);
      _mm_store_si128((__m128i *) c, cs);
    }

    void SIMDRuntime::SIMDCmpGt4(int32_t *a, int32_t *b, int32_t *c) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_cmpgt_epi32(as, bs);
      _mm_store_si128((__m128i *) c, cs);
    }

    void SIMDRuntime::SIMDFAdd4(float *a, float *b, float *c) {
      __m128 as = _mm_load_ps(a);
      __m128 bs = _mm_load_ps(b);
      __m128 cs = _mm_add_ps(as, bs);
      _mm_store_ps(c, cs);
    }

    void SIMDRuntime::SIMDFSub4(float *a, float *b, float *c) {
      __m128 as = _mm_load_ps(a);
      __m128 bs = _mm_load_ps(b);
      __m128 cs = _mm_sub_ps(as, bs);
      _mm_store_ps(c, cs);
    }

  }
}