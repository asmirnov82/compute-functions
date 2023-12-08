using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.NumberDataProviders
{
    internal unsafe struct ConvertedNumbersFromSpanDataProvider<TResult, T, TConverter> : INumberDataProvider<TResult>
        where TResult : unmanaged, INumber<TResult>
        where T : unmanaged, INumber<T>
        where TConverter : unmanaged, IConverter<TResult, T>
    {
        private readonly T* _ref;

        public ConvertedNumbersFromSpanDataProvider(T* source)
        {
            _ref = source;
        }

        public static bool SupportVectorization => TConverter.SupportVectorization;

        public TResult GetValue(int i)
        {
            return TConverter.Convert(*(_ref + i));
        }

        public Vector128<TResult> LoadVector128Unsafe(uint i)
        {
            return TConverter.Convert(Vector128.Load(_ref + i));
        }

        public Vector256<TResult> LoadVector256Unsafe(uint i)
        {
            return TConverter.Convert(Vector256.Load(_ref + i));
        }

#if NET8_0_OR_GREATER
        public Vector512<TResult> LoadVector512Unsafe(uint i)
        {
            return TConverter.Convert(Vector512.Load(_ref + i));
        }
#endif
    }
}
