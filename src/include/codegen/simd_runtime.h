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
      static inline void SIMDAdd4(int32_t *a, int32_t *b, int32_t *c);
      static inline void SIMDSub4(int32_t *a, int32_t *b, int32_t *c);
      static inline void SIMDCmpEq4(int32_t *a, int32_t *b, int32_t *c);
      static inline void SIMDCmpLt4(int32_t *a, int32_t *b, int32_t *c);
      static inline void SIMDCmpGt4(int32_t *a, int32_t *b, int32_t *c);
      static inline void SIMDFAdd4(float *a, float *b, float *c);
      static inline void SIMDFSub4(float *a, float *b, float *c);
    };

  }
}