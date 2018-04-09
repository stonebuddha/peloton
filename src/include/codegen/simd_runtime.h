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
      static inline void SIMDI32Add4(int32_t *a, int32_t *b, int32_t *c);
      static inline void SIMDI32Sub4(int32_t *a, int32_t *b, int32_t *c);
      static inline void SIMDI32CmpEq4(int32_t *a, int32_t *b, int32_t *c);
      static inline void SIMDI32CmpLt4(int32_t *a, int32_t *b, int32_t *c);
      static inline void SIMDI32CmpGt4(int32_t *a, int32_t *b, int32_t *c);

      static inline void SIMDI16CmpEq8(int16_t *a, int16_t *b, int16_t *c);

      static inline void SIMDI8CmpEq16(int8_t *a, int8_t *b, int8_t *c);

      static inline void SIMDFAdd4(float *a, float *b, float *c);
      static inline void SIMDFSub4(float *a, float *b, float *c);
    };

  }
}