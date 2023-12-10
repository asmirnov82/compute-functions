using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.Operators
{
    internal readonly struct MinOperator : IAggregationOperator
    {
        public static bool SupportVectorization => false;
        public static bool CanRightArgumentBeZero => true;
        public static T GetIdentityValue<T>() where T : unmanaged, INumber<T>, IMinMaxValue<T> => T.MaxValue;

        public static T Invoke<T>(T x, T y) where T : INumber<T> => T.MinNumber(x, y);

        public static Vector128<T> Invoke<T>(Vector128<T> x, Vector128<T> y) where T : unmanaged, INumber<T> => throw new NotImplementedException();
        public static Vector256<T> Invoke<T>(Vector256<T> x, Vector256<T> y) where T : unmanaged, INumber<T> => throw new NotImplementedException();
#if NET8_0_OR_GREATER
        public static Vector512<T> Invoke<T>(Vector512<T> x, Vector512<T> y) where T : unmanaged, INumber<T> => throw new NotImplementedException();
#endif

        public static T Invoke<T>(Vector128<T> x) where T : unmanaged, INumber<T> => throw new NotImplementedException();
        public static T Invoke<T>(Vector256<T> x) where T : unmanaged, INumber<T> => throw new NotImplementedException();
#if NET8_0_OR_GREATER
        public static T Invoke<T>(Vector512<T> x) where T : unmanaged, INumber<T> => throw new NotImplementedException();
#endif
    }
}
