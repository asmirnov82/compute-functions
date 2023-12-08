using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.NumberDataProviders
{
    internal struct NumbersFromScalarValueDataProvider<T> : INumberDataProvider<T>
        where T : unmanaged, INumber<T>
    {
        private readonly T _value;
        private readonly Vector128<T> _vector128;
        private readonly Vector256<T> _vector256;

#if NET8_0_OR_GREATER
        private readonly Vector512<T> _vector512;
#endif

        public NumbersFromScalarValueDataProvider(T value)
        {
            _value = value;

            if (Vector128<T>.IsSupported)
                _vector128 = Vector128.Create(value);

            if (Vector256<T>.IsSupported)
                _vector256 = Vector256.Create(value);

#if NET8_0_OR_GREATER
            if (Vector512<T>.IsSupported)
                _vector512 = Vector512.Create(value);
#endif
        }

        public static bool SupportVectorization => true;

        public T GetValue(int i) => _value;
        public Vector128<T> LoadVector128Unsafe(uint i) => _vector128;
        public Vector256<T> LoadVector256Unsafe(uint i) => _vector256;

#if NET8_0_OR_GREATER
        public Vector512<T> LoadVector512Unsafe(uint i) => _vector512;
#endif
    }
}
