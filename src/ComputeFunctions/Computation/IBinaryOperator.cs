using System;
using System.Numerics;
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
        /// <summary>
        /// True if operator can be vectorized using SIMD commands.
        /// </summary>
        static abstract bool SupportVectorization { get; }

        /// <summary>
        /// True if rigth argumant can be zero. For example it's not possible for Divide and Modulo operators.
        /// </summary>
        static abstract bool CanRightArgumentBeZero { get; }

        /// <summary>
        /// Invokes operator.
        /// </summary>
        static abstract T Invoke<T>(T x, T y) where T : INumber<T>;

        /// <summary>
        /// Invokes operator on 128-bit SIMD vectors.
        /// </summary>
        static abstract Vector128<T> Invoke<T>(Vector128<T> x, Vector128<T> y) where T : unmanaged, INumber<T>;

        /// <summary>
        /// Invokes operator on 256-bit SIMD vectors.
        /// </summary>
        static abstract Vector256<T> Invoke<T>(Vector256<T> x, Vector256<T> y) where T : unmanaged, INumber<T>;

#if NET8_0_OR_GREATER
        /// <summary>
        /// Invokes operator on 512-bit SIMD vectors.
        /// </summary>
        static abstract Vector512<T> Invoke<T>(Vector512<T> x, Vector512<T> y) where T : unmanaged, INumber<T>;
#endif
    }
}
