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

    void SIMDRuntime::SIMDI32Add4(int32_t *a, int32_t *b, int32_t *c) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_add_epi32(as, bs);
      _mm_store_si128((__m128i *) c, cs);
    }

    void SIMDRuntime::SIMDI32Sub4(int32_t *a, int32_t *b, int32_t *c) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_sub_epi32(as, bs);
      _mm_store_si128((__m128i *) c, cs);
    }

    void SIMDRuntime::SIMDI32CmpEq4(int32_t *a, int32_t *b, int32_t *c) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_cmpeq_epi32(as, bs);
      _mm_store_si128((__m128i *) c, cs);
    }

    void SIMDRuntime::SIMDI32CmpLt4(int32_t *a, int32_t *b, int32_t *c) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_cmplt_epi32(as, bs);
      _mm_store_si128((__m128i *) c, cs);
    }

    void SIMDRuntime::SIMDI32CmpGt4(int32_t *a, int32_t *b, int32_t *c) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_cmpgt_epi32(as, bs);
      _mm_store_si128((__m128i *) c, cs);
    }

    void SIMDRuntime::SIMDI16CmpEq8(int16_t *a, int16_t *b, int16_t *c) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_cmpeq_epi16(as, bs);
      _mm_store_si128((__m128i *) c, cs);
    }

    void SIMDRuntime::SIMDI8CmpEq16(int8_t *a, int8_t *b, int8_t *c) {
      __m128i as = _mm_load_si128((const __m128i *) a);
      __m128i bs = _mm_load_si128((const __m128i *) b);
      __m128i cs = _mm_cmpeq_epi8(as, bs);
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