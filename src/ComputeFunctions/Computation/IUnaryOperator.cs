using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation
{
    /// <summary>
    /// Operator that takes one input value and returns a single value.
    /// </summary>
    internal interface IUnaryOperator
    {
        static abstract bool CanVectorize { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract T Invoke<T>(T x);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract Vector128<T> Invoke<T>(Vector128<T> x) where T : unmanaged;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract Vector256<T> Invoke<T>(Vector256<T> x) where T : unmanaged;

#if NET8_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract Vector512<T> Invoke<T>(Vector512<T> x) where T : unmanaged;
#endif
    }
}
