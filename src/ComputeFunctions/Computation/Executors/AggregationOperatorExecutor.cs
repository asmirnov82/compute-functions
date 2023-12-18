using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Apache.Arrow;

namespace Gimpo.ComputeFunctions.Computation.Executors
{
    internal class AggregationOperatorExecutor
    {
        internal static TResult InvokeOperator<TAggregationOperator, TResult>(ReadOnlySpan<TResult> x, ReadOnlySpan<byte> resultValidityBitmap, int nullCount)
            where TAggregationOperator : struct, IAggregationOperator
            where TResult : unmanaged, INumber<TResult>, IMinMaxValue<TResult>
        {
            ref TResult xRef = ref MemoryMarshal.GetReference(x);
            var result = TAggregationOperator.GetIdentityValue<TResult>();
            int i = 0;

            if (TAggregationOperator.SupportVectorization && nullCount == 0)
            {

#if NET8_0_OR_GREATER
                if (Vector512.IsHardwareAccelerated && Vector512<TResult>.IsSupported && x.Length >= Vector512<TResult>.Count)
                {
                    var resultVector = Vector512.Create(TAggregationOperator.GetIdentityValue<TResult>());
                    int vectorSize = Vector512<TResult>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;

                    // Aggregate additional vectors into the result as long as there's at
                    // least one full vector left to process.
                    do
                    {
                        resultVector = TAggregationOperator.Invoke(resultVector, Vector512.LoadUnsafe(ref xRef, (uint)i));
                        i += vectorSize;
                    }
                    while (i <= oneVectorFromEnd);

                    // Aggregate the lanes in the vector back into the scalar result
                    result = TAggregationOperator.Invoke(result, TAggregationOperator.Invoke(resultVector));
                }
#endif
                if (Vector256.IsHardwareAccelerated && Vector256<TResult>.IsSupported && x.Length - i >= Vector256<TResult>.Count)
                {
                    var resultVector = Vector256.Create(TAggregationOperator.GetIdentityValue<TResult>());
                    int vectorSize = Vector256<TResult>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;

                    // Aggregate additional vectors into the result as long as there's at
                    // least one full vector left to process.
                    do
                    {
                        resultVector = TAggregationOperator.Invoke(resultVector, Vector256.LoadUnsafe(ref xRef, (uint)i));
                        i += vectorSize;
                    }
                    while (i <= oneVectorFromEnd);

                    // Aggregate the lanes in the vector back into the scalar result
                    result = TAggregationOperator.Invoke(result, TAggregationOperator.Invoke(resultVector));
                }

                if (Vector128.IsHardwareAccelerated && Vector128<TResult>.IsSupported && x.Length - i >= Vector128<TResult>.Count)
                {
                    var resultVector = Vector128.Create(TAggregationOperator.GetIdentityValue<TResult>());
                    int vectorSize = Vector128<TResult>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;

                    // Aggregate additional vectors into the result as long as there's at
                    // least one full vector left to process.
                    do
                    {
                        resultVector = TAggregationOperator.Invoke(resultVector, Vector128.LoadUnsafe(ref xRef, (uint)i));
                        i += vectorSize;
                    }
                    while (i <= oneVectorFromEnd);

                    // Aggregate the lanes in the vector back into the scalar result
                    result = TAggregationOperator.Invoke(result, TAggregationOperator.Invoke(resultVector));
                }
            }

            // Aggregate the remaining items in the input span
            if (nullCount == 0)
                for (; (uint)i < (uint)x.Length; i++)
                {
                    result = TAggregationOperator.Invoke(result, x[i]);
                }
            else
                for (; (uint)i < (uint)x.Length; i++)
                {
                    if (BitUtility.GetBit(resultValidityBitmap, i))
                        result = TAggregationOperator.Invoke(result, x[i]);
                }

            return result;
        }

        internal static TResult InvokeOperatorWithWidening<TAggregationOperator, TResult, T, TWidener>(ReadOnlySpan<T> x, ReadOnlySpan<byte> resultValidityBitmap, int nullCount)
            where TAggregationOperator : struct, IAggregationOperator
            where TResult : unmanaged, INumber<TResult>, IMinMaxValue<TResult>
            where T : unmanaged, INumber<T>
            where TWidener: unmanaged, IWidener<TResult, T>
        {
            ref T xRef = ref MemoryMarshal.GetReference(x);
            var result = TAggregationOperator.GetIdentityValue<TResult>();
            int i = 0;

            if (TAggregationOperator.SupportVectorization && nullCount == 0)
            {
#if NET8_0_OR_GREATER
                if (Vector512.IsHardwareAccelerated && Vector512<TResult>.IsSupported && x.Length >= Vector512<T>.Count)
                {
                    var resultVector = Vector512.Create(TAggregationOperator.GetIdentityValue<TResult>());
                    int vectorSize = Vector512<T>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;

                    // Aggregate additional vectors into the result as long as there's at
                    // least one full vector left to process.
                    do
                    {
                        var (lower, upper) = TWidener.Widen(Vector512.LoadUnsafe(ref xRef, (uint)i));

                        resultVector = TAggregationOperator.Invoke(resultVector, lower);
                        resultVector = TAggregationOperator.Invoke(resultVector, upper);

                        i += vectorSize;
                    }
                    while (i <= oneVectorFromEnd);

                    // Aggregate the lanes in the vector back into the scalar result
                    result = TAggregationOperator.Invoke(result, TAggregationOperator.Invoke(resultVector));
                }
#endif
                if (Vector256.IsHardwareAccelerated && Vector256<T>.IsSupported && x.Length - i >= Vector256<T>.Count)
                {
                    var resultVector = Vector256.Create(TAggregationOperator.GetIdentityValue<TResult>());
                    int vectorSize = Vector256<T>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;

                    // Aggregate additional vectors into the result as long as there's at
                    // least one full vector left to process.
                    do
                    {
                        var (lower, upper) = TWidener.Widen(Vector256.LoadUnsafe(ref xRef, (uint)i));

                        resultVector = TAggregationOperator.Invoke(resultVector, lower);
                        resultVector = TAggregationOperator.Invoke(resultVector, upper);

                        i += vectorSize;
                    }
                    while (i <= oneVectorFromEnd);

                    // Aggregate the lanes in the vector back into the scalar result
                    result = TAggregationOperator.Invoke(result, TAggregationOperator.Invoke(resultVector));
                }

                if (Vector128.IsHardwareAccelerated && Vector128<TResult>.IsSupported && x.Length - i >= Vector128<T>.Count)
                {
                    var resultVector = Vector128.Create(TAggregationOperator.GetIdentityValue<TResult>());
                    int vectorSize = Vector128<T>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;

                    // Aggregate additional vectors into the result as long as there's at
                    // least one full vector left to process.
                    do
                    {
                        var (lower, upper) = TWidener.Widen(Vector128.LoadUnsafe(ref xRef, (uint)i));

                        resultVector = TAggregationOperator.Invoke(resultVector, lower);
                        resultVector = TAggregationOperator.Invoke(resultVector, upper);

                        i += vectorSize;
                    }
                    while (i <= oneVectorFromEnd);

                    // Aggregate the lanes in the vector back into the scalar result
                    result = TAggregationOperator.Invoke(result, TAggregationOperator.Invoke(resultVector));
                }
            }

            // Aggregate the remaining items in the input span
            if (nullCount == 0)
                for (; (uint)i < (uint)x.Length; i++)
                {
                    result = TAggregationOperator.Invoke(result, TWidener.Widen(x[i]));
                }
            else
                for (; (uint)i < (uint)x.Length; i++)
                {
                    if (BitUtility.GetBit(resultValidityBitmap, i))
                        result = TAggregationOperator.Invoke(result, TWidener.Widen(x[i]));
                }

            return result;
        }
        
        internal static TResult InvokeOperator<TAggregationOperator, TResult, TWidened, T, TConverter, TWidener>(ReadOnlySpan<T> x, ReadOnlySpan<byte> resultValidityBitmap, int nullCount)
            where TAggregationOperator : struct, IAggregationOperator
            where TResult : unmanaged, INumber<TResult>, IMinMaxValue<TResult>
            where TWidened : unmanaged, INumber<TWidened>
            where T : unmanaged, INumber<T>
            where TConverter: unmanaged, IConverter<TResult, TWidened>
            where TWidener : unmanaged, IWidener<TWidened, T>
        {
            ref T xRef = ref MemoryMarshal.GetReference(x);
            var result = TAggregationOperator.GetIdentityValue<TResult>();
            int i = 0;

            if (TAggregationOperator.SupportVectorization && TConverter.SupportVectorization && nullCount == 0)
            {
#if NET8_0_OR_GREATER
                if (Vector512.IsHardwareAccelerated && Vector512<T>.IsSupported && x.Length >= Vector512<T>.Count)
                {
                    int vectorSize = Vector512<T>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;

                    // Aggregate additional vectors into the result as long as there's at
                    // least one full vector left to process.
                    do
                    {
                        var (lower, upper) = TWidener.Widen(Vector512.LoadUnsafe(ref xRef, (uint)i));
                        var resultVector = TAggregationOperator.Invoke(lower, upper);

                        // Aggregate the lanes in the vector back into the scalar result
                        result = TAggregationOperator.Invoke(result, TConverter.Convert(TAggregationOperator.Invoke(resultVector)));

                        i += vectorSize;
                    }
                    while (i <= oneVectorFromEnd);
                }
#endif

                if (Vector256.IsHardwareAccelerated && Vector256<T>.IsSupported && x.Length - i >= Vector256<T>.Count)
                {
                    int vectorSize = Vector256<T>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;

                    // Aggregate additional vectors into the result as long as there's at
                    // least one full vector left to process.
                    do
                    {
                        var (lower, upper) = TWidener.Widen(Vector256.LoadUnsafe(ref xRef, (uint)i));
                        var resultVector = TAggregationOperator.Invoke(lower, upper);

                        // Aggregate the lanes in the vector back into the scalar result
                        result = TAggregationOperator.Invoke(result, TConverter.Convert(TAggregationOperator.Invoke(resultVector)));

                        i += vectorSize;
                    }
                    while (i <= oneVectorFromEnd);
                }

                if (Vector128.IsHardwareAccelerated && Vector128<T>.IsSupported && x.Length - i >= Vector128<T>.Count)
                {
                    int vectorSize = Vector128<T>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;

                    // Aggregate additional vectors into the result as long as there's at
                    // least one full vector left to process
                    do
                    {
                        var (lower, upper) = TWidener.Widen(Vector128.LoadUnsafe(ref xRef, (uint)i));
                        var resultVector = TAggregationOperator.Invoke(lower, upper);

                        // Aggregate the lanes in the vector back into the scalar result
                        result = TAggregationOperator.Invoke(result, TConverter.Convert(TAggregationOperator.Invoke(resultVector)));

                        i += vectorSize;
                    }
                    while (i <= oneVectorFromEnd);
                }
            }

            // Aggregate the remaining items in the input span
            if (nullCount == 0)
                for (; (uint)i < (uint)x.Length; i++)
                {
                    result = TAggregationOperator.Invoke(result, TConverter.Convert(TWidener.Widen(x[i])));
                }
            else
                for (; (uint)i < (uint)x.Length; i++)
                {
                    if (BitUtility.GetBit(resultValidityBitmap, i))
                        result = TAggregationOperator.Invoke(result, TConverter.Convert(TWidener.Widen(x[i])));
                }

            return result;
        }

        internal static TResult InvokeOperator<TAggregationOperator, TResult, T, TConverter>(ReadOnlySpan<T> x, ReadOnlySpan<byte> resultValidityBitmap, int nullCount)
            where TAggregationOperator : struct, IAggregationOperator
            where TResult : unmanaged, INumber<TResult>, IMinMaxValue<TResult>
            where T : unmanaged, INumber<T>
            where TConverter : unmanaged, IConverter<TResult, T>
        {
            var result = TAggregationOperator.GetIdentityValue<TResult>();

            // Aggregate the remaining items in the input span
            if (nullCount == 0)
                for (int i = 0; (uint)i < (uint)x.Length; i++)
                {
                    result = TAggregationOperator.Invoke(result, TConverter.Convert(x[i]));
                }
            else
                for (int i = 0; (uint)i < (uint)x.Length; i++)
                {
                    if (BitUtility.GetBit(resultValidityBitmap, i))
                        result = TAggregationOperator.Invoke(result, TConverter.Convert(x[i]));
                }

            return result;
        }
    }
}
