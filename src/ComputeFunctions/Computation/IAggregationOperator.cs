using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation
{
    /// <summary>
    /// <see cref="IBinaryOperator"/> that specializes horizontal aggregation of all elements in a vector.
    /// </summary>
    public interface IAggregationOperator : IBinaryOperator
    {
        static abstract T GetIdentityValue<T>() where T : unmanaged, INumber<T>, IMinMaxValue<T>;

        static abstract T Invoke<T>(Vector128<T> x) where T : unmanaged, INumber<T>;
        static abstract T Invoke<T>(Vector256<T> x) where T : unmanaged, INumber<T>;
#if NET8_0_OR_GREATER
        static abstract T Invoke<T>(Vector512<T> x) where T : unmanaged, INumber<T>;
#endif
    }
}
