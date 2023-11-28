﻿using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;


namespace Gimpo.ComputeFunctions.Computation
{
    /// <summary>
    /// Converts a vector of a specified numeric type into different type without widening or narrowing.
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <typeparam name="T"></typeparam>
    internal interface IConverter<TResult, T>
        where TResult : unmanaged
        where T : unmanaged
    {
        /// <summary>
        /// True if conversion is suitable for low-level optimization using SIMD instructions.
        /// </summary>
        static abstract bool CanVectorize { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract TResult Convert(T value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract Vector128<TResult> Convert(Vector128<T> vector);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract Vector256<TResult> Convert (Vector256<T> vector);

#if NET8_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract Vector512<TResult> Convert(Vector512<T> vector);
#endif
    }
}
