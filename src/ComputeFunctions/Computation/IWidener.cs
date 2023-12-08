using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace Gimpo.ComputeFunctions.Computation
{
    /// <summary>
    /// Widens a vector of a specified numeric type that is suitable for low-level optimization.
    /// </summary>
    /// <typeparam name="TResult">The type of the widened vectors.</typeparam>
    /// <typeparam name="T">The type of the vector whose elements are to be widened.</typeparam>
    internal interface IWidener<TResult, T> 
        where TResult : unmanaged
        where T : unmanaged
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract TResult Widen(T value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract (Vector128<TResult> Lower, Vector128<TResult> Upper) Widen(Vector128<T> vector);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract (Vector256<TResult> Lower, Vector256<TResult> Upper) Widen(Vector256<T> vector);

#if NET8_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract (Vector512<TResult> Lower, Vector512<TResult> Upper) Widen(Vector512<T> vector);
#endif
    }
}
