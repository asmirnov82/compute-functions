using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation
{
    /// <summary>
    /// Operator that takes two input values and returns a single value.
    /// </summary>
    public interface IBinaryOperator
    {
        static abstract bool CanVectorize { get; }

        static abstract bool CanRightArgumentBeZero { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract T Invoke<T>(T x, T y) where T : INumber<T>;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract Vector128<T> Invoke<T>(Vector128<T> x, Vector128<T> y) where T : unmanaged, INumber<T>;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract Vector256<T> Invoke<T>(Vector256<T> x, Vector256<T> y) where T : unmanaged, INumber<T>;

#if NET8_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static abstract Vector512<T> Invoke<T>(Vector512<T> x, Vector512<T> y) where T : unmanaged, INumber<T>;
#endif
    }
}
