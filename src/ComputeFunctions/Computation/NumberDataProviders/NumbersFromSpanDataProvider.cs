using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.NumberDataProviders
{
    internal unsafe struct NumbersFromSpanDataProvider<T> : INumberDataProvider<T>
        where T : unmanaged, INumber<T>
    {
        private readonly T* _ref;

        public NumbersFromSpanDataProvider(T* source)
        {
            _ref = source;
        }

        public static bool SupportVectorization => true;

        public T GetValue(int i)
        {
            return *(_ref + i);
        }

        public Vector128<T> LoadVector128Unsafe(uint i)
        {
            return Vector128.Load(_ref + i);
        }

        public Vector256<T> LoadVector256Unsafe(uint i)
        {
            return Vector256.Load(_ref + i);
        }

#if NET8_0_OR_GREATER
        public Vector512<T> LoadVector512Unsafe(uint i)
        {
            return Vector512.Load(_ref + i);
        }
#endif
    }
}
