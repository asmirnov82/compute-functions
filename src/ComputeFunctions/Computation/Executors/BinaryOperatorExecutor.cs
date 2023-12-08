using Apache.Arrow;
using Gimpo.ComputeFunctions.Computation.NumberDataProviders;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Gimpo.ComputeFunctions.Computation.Executors
{
    internal static class BinaryOperatorExecutor
    {
        private static void InvokeOperator<TBinaryOperator, TResult, TNumberDataProviderX, TNumberDataProviderY>(TNumberDataProviderX x, TNumberDataProviderY y, int length, ReadOnlySpan<byte> resultValidityBitmap, int nullCount, Span<TResult> destination)
            where TBinaryOperator : struct, IBinaryOperator
            where TResult : unmanaged, INumber<TResult>
            where TNumberDataProviderX : struct, INumberDataProvider<TResult>
            where TNumberDataProviderY : struct, INumberDataProvider<TResult>
        {
            ref var dRef = ref MemoryMarshal.GetReference(destination);

            if (TBinaryOperator.SupportVectorization && (TBinaryOperator.CanRightArgumentBeZero || nullCount == 0) && TNumberDataProviderX.SupportVectorization && TNumberDataProviderY.SupportVectorization)
            {
                if (InvokeOperatorVectorized<TBinaryOperator, TResult, TNumberDataProviderX, TNumberDataProviderY>(x, y, length, ref dRef))
                    return;
            }

            int i = 0;
            if (nullCount == 0)
                while (i < length)
                {
                    Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(x.GetValue(i), y.GetValue(i));
                    i++;
                }
            else
                while (i < length)
                {
                    if (BitUtility.GetBit(resultValidityBitmap, i))
                    {
                        Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(x.GetValue(i), y.GetValue(i));
                    }
                    i++;
                }
        }

        private static bool InvokeOperatorVectorized<TBinaryOperator, TResult, TNumberVectorProviderX, TNumberVectorProviderY>(TNumberVectorProviderX x, TNumberVectorProviderY y, int length, ref TResult destination)
            where TBinaryOperator : struct, IBinaryOperator
            where TResult : unmanaged, INumber<TResult>
            where TNumberVectorProviderX : struct, INumberVectorProvider<TResult>
            where TNumberVectorProviderY : struct, INumberVectorProvider<TResult>
        {
#if NET8_0_OR_GREATER
            if (Vector512.IsHardwareAccelerated && Vector512<TResult>.IsSupported)
            {
                int i = 0;
                int vectorSize = Vector512<TResult>.Count;
                int oneVectorFromEnd = length - vectorSize;
                if (i <= oneVectorFromEnd)
                {
                    // Loop handling one vector at a time.
                    do
                    {
                        TBinaryOperator.Invoke(x.LoadVector512Unsafe((uint)i), y.LoadVector512Unsafe((uint)i))
                            .StoreUnsafe(ref destination, (uint)i);

                        i += vectorSize;
                    }
                    while (i <= oneVectorFromEnd);

                    // Handle any remaining elements with a final vector.
                    if (i != length)
                    {
                        uint lastVectorIndex = (uint)oneVectorFromEnd;
                        TBinaryOperator.Invoke(x.LoadVector512Unsafe(lastVectorIndex), y.LoadVector512Unsafe(lastVectorIndex))
                            .StoreUnsafe(ref destination, lastVectorIndex);
                    }

                    return true;
                }
            }
#endif
            if (Vector256.IsHardwareAccelerated && Vector256<TResult>.IsSupported)
            {
                int i = 0;
                int vectorSize = Vector256<TResult>.Count;
                int oneVectorFromEnd = length - vectorSize;
                if (i <= oneVectorFromEnd)
                {
                    // Loop handling one vector at a time.
                    do
                    {
                        TBinaryOperator.Invoke(x.LoadVector256Unsafe((uint)i), y.LoadVector256Unsafe((uint)i))
                            .StoreUnsafe(ref destination, (uint)i);

                        i += vectorSize;
                    }
                    while (i <= oneVectorFromEnd);

                    // Handle any remaining elements with a final vector.
                    if (i != length)
                    {
                        uint lastVectorIndex = (uint)oneVectorFromEnd;
                        TBinaryOperator.Invoke(x.LoadVector256Unsafe(lastVectorIndex), y.LoadVector256Unsafe(lastVectorIndex))
                            .StoreUnsafe(ref destination, lastVectorIndex);
                    }

                    return true;
                }
            }

            if (Vector128.IsHardwareAccelerated && Vector128<TResult>.IsSupported)
            {
                int i = 0;
                int vectorSize = Vector128<TResult>.Count;
                int oneVectorFromEnd = length - vectorSize;
                if (i <= oneVectorFromEnd)
                {
                    // Loop handling one vector at a time.
                    do
                    {
                        TBinaryOperator.Invoke(x.LoadVector128Unsafe((uint)i), y.LoadVector128Unsafe((uint)i))
                            .StoreUnsafe(ref destination, (uint)i);

                        i += vectorSize;
                    }
                    while (i <= oneVectorFromEnd);

                    // Handle any remaining elements with a final vector.
                    if (i != length)
                    {
                        uint lastVectorIndex = (uint)oneVectorFromEnd;
                        TBinaryOperator.Invoke(x.LoadVector128Unsafe(lastVectorIndex), y.LoadVector128Unsafe(lastVectorIndex))
                            .StoreUnsafe(ref destination, lastVectorIndex);
                    }

                    return true;
                }
            }

            return false;
        }

        #region At least one argument is a Scalar
        internal unsafe static void InvokeOperator<TBinaryOperator, TResult>(ReadOnlySpan<TResult> array, TResult scalar, ReadOnlySpan<byte> resultValidityBitmap, int nullCount, Span<TResult> destination, bool reverseArguments = false)
            where TBinaryOperator : struct, IBinaryOperator
            where TResult : unmanaged, INumber<TResult>
        {
            if (reverseArguments)
            {                
                fixed (TResult* ptr = array)
                {
                    InvokeOperator<TBinaryOperator, TResult, NumbersFromScalarValueDataProvider<TResult>, NumbersFromSpanDataProvider<TResult>>(
                        new NumbersFromScalarValueDataProvider<TResult>(scalar),
                        new NumbersFromSpanDataProvider<TResult>(ptr),
                        array.Length,
                        resultValidityBitmap,
                        nullCount,
                        destination);

                    return;
                }
            }
            else
            {
                fixed (TResult* ptr = array)
                {
                    InvokeOperator<TBinaryOperator, TResult, NumbersFromSpanDataProvider<TResult>, NumbersFromScalarValueDataProvider<TResult>>(
                        new NumbersFromSpanDataProvider<TResult>(ptr),
                        new NumbersFromScalarValueDataProvider<TResult>(scalar),
                        array.Length,
                        resultValidityBitmap,
                        nullCount,
                        destination);

                    return;
                }
            }
        }

        internal unsafe static void InvokeOperator<TBinaryOperator, TResult, T, TConverter>(ReadOnlySpan<T> array, TResult scalar, ReadOnlySpan<byte> resultValidityBitmap, int nullCount, Span<TResult> destination, bool reverseArguments = false)
            where TBinaryOperator : struct, IBinaryOperator
            where TResult : unmanaged, INumber<TResult>
            where T : unmanaged, INumber<T>
            where TConverter: unmanaged, IConverter<TResult, T>
        {
            if (reverseArguments)
            {
                fixed (T* ptr = array)
                {
                    InvokeOperator<TBinaryOperator, TResult, NumbersFromScalarValueDataProvider<TResult>, ConvertedNumbersFromSpanDataProvider<TResult, T, TConverter>>(new NumbersFromScalarValueDataProvider<TResult>(scalar), new ConvertedNumbersFromSpanDataProvider<TResult, T, TConverter>(ptr), array.Length, resultValidityBitmap, nullCount, destination);
                    return;
                }
            }
            else
            {
                fixed (T* ptr = array)
                {
                    InvokeOperator<TBinaryOperator, TResult, ConvertedNumbersFromSpanDataProvider<TResult, T, TConverter>, NumbersFromScalarValueDataProvider<TResult>>(new ConvertedNumbersFromSpanDataProvider<TResult, T, TConverter>(ptr), new NumbersFromScalarValueDataProvider<TResult>(scalar), array.Length, resultValidityBitmap, nullCount, destination);
                    return;
                }
            }
        }
        #endregion

        #region Both arguments are Spans
        internal static unsafe void InvokeOperator<TBinaryOperator, TResult>(ReadOnlySpan<TResult> x, ReadOnlySpan<TResult> y, ReadOnlySpan<byte> resultValidityBitmap, int nullCount, Span<TResult> destination)
            where TBinaryOperator : struct, IBinaryOperator
            where TResult : unmanaged, INumber<TResult>
        {
            fixed (TResult* ptrX = x)
            fixed (TResult* ptrY = y)
            {
                InvokeOperator<TBinaryOperator, TResult, NumbersFromSpanDataProvider<TResult>, NumbersFromSpanDataProvider<TResult>>(new NumbersFromSpanDataProvider<TResult>(ptrX), new NumbersFromSpanDataProvider<TResult>(ptrY), x.Length, resultValidityBitmap, nullCount, destination);
                return;
            }

        }
                
        internal static unsafe void InvokeOperator<TBinaryOperator, TResult, T, TConverter>(ReadOnlySpan<TResult> x, ReadOnlySpan<T> y, ReadOnlySpan<byte> resultValidityBitmap, int nullCount, Span<TResult> destination)
            where TBinaryOperator : struct, IBinaryOperator
            where TResult : unmanaged, INumber<TResult>
            where T : unmanaged, INumber<T>
            where TConverter : unmanaged, IConverter<TResult, T>
        {
            fixed (TResult* ptrX = x)
            fixed (T* ptrY = y)
            {
                InvokeOperator<TBinaryOperator, TResult, NumbersFromSpanDataProvider<TResult>, ConvertedNumbersFromSpanDataProvider<TResult, T, TConverter>>(new NumbersFromSpanDataProvider<TResult>(ptrX), new ConvertedNumbersFromSpanDataProvider<TResult, T, TConverter>(ptrY), x.Length, resultValidityBitmap, nullCount, destination);
                return;
            }
        }
        
        internal unsafe static void InvokeOperator<TBinaryOperator, TResult, T, TConverter>(ReadOnlySpan<T> x, ReadOnlySpan<TResult> y, ReadOnlySpan<byte> resultValidityBitmap, int nullCount, Span<TResult> destination)
            where TBinaryOperator : struct, IBinaryOperator
            where TResult : unmanaged, INumber<TResult>
            where T : unmanaged, INumber<T>
            where TConverter : unmanaged, IConverter<TResult, T>
        {
            fixed (T* ptrX = x)
            fixed (TResult* ptrY = y)
            {
                InvokeOperator<TBinaryOperator, TResult, ConvertedNumbersFromSpanDataProvider<TResult, T, TConverter>, NumbersFromSpanDataProvider<TResult>>(new ConvertedNumbersFromSpanDataProvider<TResult, T, TConverter>(ptrX), new NumbersFromSpanDataProvider<TResult>(ptrY), x.Length, resultValidityBitmap, nullCount, destination);
                return;
            }
        }

        internal unsafe static void InvokeOperator<TBinaryOperator, TResult, TX, TY, TConverterX, TConverterY>(ReadOnlySpan<TX> x, ReadOnlySpan<TY> y, ReadOnlySpan<byte> resultValidityBitmap, int nullCount, Span<TResult> destination)
          where TBinaryOperator : struct, IBinaryOperator
          where TResult : unmanaged, INumber<TResult>
          where TX : unmanaged, INumber<TX>
          where TY : unmanaged, INumber<TY>
          where TConverterX : unmanaged, IConverter<TResult, TX>
          where TConverterY : unmanaged, IConverter<TResult, TY>
        {
            fixed (TX* ptrX = x)
            fixed (TY* ptrY = y)
            {
                InvokeOperator<TBinaryOperator, TResult, ConvertedNumbersFromSpanDataProvider<TResult, TX, TConverterX>, ConvertedNumbersFromSpanDataProvider<TResult, TY, TConverterY>>(new ConvertedNumbersFromSpanDataProvider<TResult, TX, TConverterX>(ptrX), new ConvertedNumbersFromSpanDataProvider<TResult, TY, TConverterY>(ptrY), x.Length, resultValidityBitmap, nullCount, destination);
                return;
            }
        }

        internal static void InvokeOperatorWithWidening<TBinaryOperator, TResult, T, TWidener>(ReadOnlySpan<TResult> x, ReadOnlySpan<T> y, ReadOnlySpan<byte> resultValidityBitmap, int nullCount, Span<TResult> destination)
           where TBinaryOperator : struct, IBinaryOperator
           where TResult : unmanaged, INumber<TResult>
           where T : unmanaged, INumber<T>
           where TWidener : struct, IWidener<TResult, T>
        {
            ref var xRef = ref MemoryMarshal.GetReference(x);
            ref var yRef = ref MemoryMarshal.GetReference(y);
            ref var dRef = ref MemoryMarshal.GetReference(destination);
            int i = 0;

            if (TBinaryOperator.SupportVectorization && (TBinaryOperator.CanRightArgumentBeZero || nullCount == 0))
            {
#if NET8_0_OR_GREATER
                if (Vector512.IsHardwareAccelerated && Vector512<TResult>.IsSupported)
                {
                    int vectorSize = Vector512<TResult>.Count;
                    int oneVectorFromEnd = x.Length - 2 * vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            var (lower, upper) = TWidener.Widen(Vector512.LoadUnsafe(ref yRef, (uint)i));

                            TBinaryOperator.Invoke(Vector512.LoadUnsafe(ref xRef, (uint)i), lower).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;

                            TBinaryOperator.Invoke(Vector512.LoadUnsafe(ref xRef, (uint)i), upper).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            var (lower, upper) = TWidener.Widen(Vector512.LoadUnsafe(ref yRef, lastVectorIndex));

                            TBinaryOperator.Invoke(Vector512.LoadUnsafe(ref xRef, lastVectorIndex), lower).StoreUnsafe(ref dRef, lastVectorIndex);
                            lastVectorIndex += (uint)vectorSize;

                            TBinaryOperator.Invoke(Vector512.LoadUnsafe(ref xRef, lastVectorIndex), upper).StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }
#endif

                if (Vector256.IsHardwareAccelerated && Vector256<TResult>.IsSupported)
                {
                    int vectorSize = Vector256<TResult>.Count;
                    int oneVectorFromEnd = x.Length - 2 * vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            var (lower, upper) = TWidener.Widen(Vector256.LoadUnsafe(ref yRef, (uint)i));

                            TBinaryOperator.Invoke(Vector256.LoadUnsafe(ref xRef, (uint)i), lower).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;

                            TBinaryOperator.Invoke(Vector256.LoadUnsafe(ref xRef, (uint)i), upper).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            var (lower, upper) = TWidener.Widen(Vector256.LoadUnsafe(ref yRef, lastVectorIndex));

                            TBinaryOperator.Invoke(Vector256.LoadUnsafe(ref xRef, lastVectorIndex), lower).StoreUnsafe(ref dRef, lastVectorIndex);
                            lastVectorIndex += (uint)vectorSize;

                            TBinaryOperator.Invoke(Vector256.LoadUnsafe(ref xRef, lastVectorIndex), upper).StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }

                if (Vector128.IsHardwareAccelerated && Vector128<TResult>.IsSupported)
                {
                    int vectorSize = Vector128<TResult>.Count;
                    int oneVectorFromEnd = x.Length - 2 * vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            var (lower, upper) = TWidener.Widen(Vector128.LoadUnsafe(ref yRef, (uint)i));

                            TBinaryOperator.Invoke(Vector128.LoadUnsafe(ref xRef, (uint)i), lower).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;

                            TBinaryOperator.Invoke(Vector128.LoadUnsafe(ref xRef, (uint)i), upper).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            var (lower, upper) = TWidener.Widen(Vector128.LoadUnsafe(ref yRef, lastVectorIndex));

                            TBinaryOperator.Invoke(Vector128.LoadUnsafe(ref xRef, lastVectorIndex), lower).StoreUnsafe(ref dRef, lastVectorIndex);
                            lastVectorIndex += (uint)vectorSize;

                            TBinaryOperator.Invoke(Vector128.LoadUnsafe(ref xRef, lastVectorIndex), upper).StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }
            }

            if (nullCount == 0)
                while (i < x.Length)
                {
                    Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(Unsafe.Add(ref xRef, i),
                                                                     TWidener.Widen(Unsafe.Add(ref yRef, i)));
                    i++;
                }
            else
                while (i < x.Length)
                {
                    if (BitUtility.GetBit(resultValidityBitmap, i))
                    {
                        Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(Unsafe.Add(ref xRef, i),
                                                                         TWidener.Widen(Unsafe.Add(ref yRef, i)));
                    }
                    i++;
                }
        }

        internal static void InvokeOperatorWithWidening<TBinaryOperator, TResult, T, TWidener>(ReadOnlySpan<T> x, ReadOnlySpan<TResult> y, ReadOnlySpan<byte> resultValidityBitmap, int nullCount, Span<TResult> destination)
          where TBinaryOperator : struct, IBinaryOperator
          where TResult : unmanaged, INumber<TResult>
          where T : unmanaged, INumber<T>
          where TWidener : struct, IWidener<TResult, T>
        {
            ref var xRef = ref MemoryMarshal.GetReference(x);
            ref var yRef = ref MemoryMarshal.GetReference(y);
            ref var dRef = ref MemoryMarshal.GetReference(destination);
            int i = 0;

            if (TBinaryOperator.SupportVectorization && (TBinaryOperator.CanRightArgumentBeZero || nullCount == 0))
            {
#if NET8_0_OR_GREATER
                if (Vector512.IsHardwareAccelerated && Vector512<TResult>.IsSupported)
                {
                    int vectorSize = Vector512<TResult>.Count;
                    int oneVectorFromEnd = x.Length - 2 * vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            var (lower, upper) = TWidener.Widen(Vector512.LoadUnsafe(ref xRef, (uint)i));

                            TBinaryOperator.Invoke(lower, Vector512.LoadUnsafe(ref yRef, (uint)i)).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;

                            TBinaryOperator.Invoke(upper, Vector512.LoadUnsafe(ref yRef, (uint)i)).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            var (lower, upper) = TWidener.Widen(Vector512.LoadUnsafe(ref xRef, lastVectorIndex));

                            TBinaryOperator.Invoke(lower, Vector512.LoadUnsafe(ref yRef, lastVectorIndex)).StoreUnsafe(ref dRef, lastVectorIndex);
                            lastVectorIndex += (uint)vectorSize;

                            TBinaryOperator.Invoke(upper, Vector512.LoadUnsafe(ref yRef, lastVectorIndex)).StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }
#endif

                if (Vector256.IsHardwareAccelerated && Vector256<TResult>.IsSupported)
                {
                    int vectorSize = Vector256<TResult>.Count;
                    int oneVectorFromEnd = x.Length - 2 * vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            var (lower, upper) = TWidener.Widen(Vector256.LoadUnsafe(ref xRef, (uint)i));

                            TBinaryOperator.Invoke(lower, Vector256.LoadUnsafe(ref yRef, (uint)i)).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;

                            TBinaryOperator.Invoke(upper, Vector256.LoadUnsafe(ref yRef, (uint)i)).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            var (lower, upper) = TWidener.Widen(Vector256.LoadUnsafe(ref xRef, lastVectorIndex));

                            TBinaryOperator.Invoke(lower, Vector256.LoadUnsafe(ref yRef, lastVectorIndex)).StoreUnsafe(ref dRef, lastVectorIndex);
                            lastVectorIndex += (uint)vectorSize;

                            TBinaryOperator.Invoke(upper, Vector256.LoadUnsafe(ref yRef, lastVectorIndex)).StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }

                if (Vector128.IsHardwareAccelerated && Vector128<TResult>.IsSupported)
                {
                    int vectorSize = Vector128<TResult>.Count;
                    int oneVectorFromEnd = x.Length - 2 * vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            var (lower, upper) = TWidener.Widen(Vector128.LoadUnsafe(ref xRef, (uint)i));

                            TBinaryOperator.Invoke(lower, Vector128.LoadUnsafe(ref yRef, (uint)i)).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;

                            TBinaryOperator.Invoke(upper, Vector128.LoadUnsafe(ref yRef, (uint)i)).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            var (lower, upper) = TWidener.Widen(Vector128.LoadUnsafe(ref xRef, lastVectorIndex));

                            TBinaryOperator.Invoke(lower, Vector128.LoadUnsafe(ref yRef, lastVectorIndex)).StoreUnsafe(ref dRef, lastVectorIndex);
                            lastVectorIndex += (uint)vectorSize;

                            TBinaryOperator.Invoke(upper, Vector128.LoadUnsafe(ref yRef, lastVectorIndex)).StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }
            }

            if (nullCount == 0)
                while (i < x.Length)
                {
                    Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(TWidener.Widen(Unsafe.Add(ref xRef, i)),
                                                                     Unsafe.Add(ref yRef, i));
                    i++;
                }
            else
                while (i < x.Length)
                {
                    if (BitUtility.GetBit(resultValidityBitmap, i))
                    {
                        Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(TWidener.Widen(Unsafe.Add(ref xRef, i)),
                                                                         Unsafe.Add(ref yRef, i));
                    }
                    i++;
                }
        }

        internal static void InvokeOperatorWithWidening<TBinaryOperator, TResult, TX, TY, TWidenerX, TWidenerY>(ReadOnlySpan<TX> x, ReadOnlySpan<TY> y, ReadOnlySpan<byte> resultValidityBitmap, int nullCount, Span<TResult> destination)
           where TBinaryOperator : struct, IBinaryOperator
           where TResult : unmanaged, INumber<TResult>
           where TX : unmanaged, INumber<TX>
           where TY : unmanaged, INumber<TY>
           where TWidenerX : struct, IWidener<TResult, TX>
           where TWidenerY : struct, IWidener<TResult, TY>
        {
            ref var xRef = ref MemoryMarshal.GetReference(x);
            ref var yRef = ref MemoryMarshal.GetReference(y);
            ref var dRef = ref MemoryMarshal.GetReference(destination);
            int i = 0;

            if (TBinaryOperator.SupportVectorization && (TBinaryOperator.CanRightArgumentBeZero || nullCount == 0))
            {
#if NET8_0_OR_GREATER
                if (Vector512.IsHardwareAccelerated && Vector512<TResult>.IsSupported)
                {
                    int vectorSize = Vector512<TResult>.Count;
                    int oneVectorFromEnd = x.Length - 2 * vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            var (lowerX, upperX) = TWidenerX.Widen(Vector512.LoadUnsafe(ref xRef, (uint)i));
                            var (lowerY, upperY) = TWidenerY.Widen(Vector512.LoadUnsafe(ref yRef, (uint)i));

                            TBinaryOperator.Invoke(lowerX, lowerY).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;

                            TBinaryOperator.Invoke(upperX, upperY).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            var (lowerX, upperX) = TWidenerX.Widen(Vector512.LoadUnsafe(ref xRef, lastVectorIndex));
                            var (lowerY, upperY) = TWidenerY.Widen(Vector512.LoadUnsafe(ref yRef, lastVectorIndex));

                            TBinaryOperator.Invoke(lowerX, lowerY).StoreUnsafe(ref dRef, lastVectorIndex);
                            lastVectorIndex += (uint)vectorSize;

                            TBinaryOperator.Invoke(upperX, upperY).StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }
#endif

                if (Vector256.IsHardwareAccelerated && Vector256<TResult>.IsSupported)
                {
                    int vectorSize = Vector256<TResult>.Count;
                    int oneVectorFromEnd = x.Length - 2 * vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            var (lowerX, upperX) = TWidenerX.Widen(Vector256.LoadUnsafe(ref xRef, (uint)i));
                            var (lowerY, upperY) = TWidenerY.Widen(Vector256.LoadUnsafe(ref yRef, (uint)i));

                            TBinaryOperator.Invoke(lowerX, lowerY).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;

                            TBinaryOperator.Invoke(upperX, upperY).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            var (lowerX, upperX) = TWidenerX.Widen(Vector256.LoadUnsafe(ref xRef, lastVectorIndex));
                            var (lowerY, upperY) = TWidenerY.Widen(Vector256.LoadUnsafe(ref yRef, lastVectorIndex));

                            TBinaryOperator.Invoke(lowerX, lowerY).StoreUnsafe(ref dRef, lastVectorIndex);
                            lastVectorIndex += (uint)vectorSize;

                            TBinaryOperator.Invoke(upperX, upperY).StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }

                if (Vector128.IsHardwareAccelerated && Vector128<TResult>.IsSupported)
                {
                    int vectorSize = Vector128<TResult>.Count;
                    int oneVectorFromEnd = x.Length - 2 * vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            var (lowerX, upperX) = TWidenerX.Widen(Vector128.LoadUnsafe(ref xRef, (uint)i));
                            var (lowerY, upperY) = TWidenerY.Widen(Vector128.LoadUnsafe(ref yRef, (uint)i));

                            TBinaryOperator.Invoke(lowerX, lowerY).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;

                            TBinaryOperator.Invoke(upperX, upperY).StoreUnsafe(ref dRef, (uint)i);
                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            var (lowerX, upperX) = TWidenerX.Widen(Vector128.LoadUnsafe(ref xRef, lastVectorIndex));
                            var (lowerY, upperY) = TWidenerY.Widen(Vector128.LoadUnsafe(ref yRef, lastVectorIndex));

                            TBinaryOperator.Invoke(lowerX, lowerY).StoreUnsafe(ref dRef, lastVectorIndex);
                            lastVectorIndex += (uint)vectorSize;

                            TBinaryOperator.Invoke(upperX, upperY).StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }
            }

            if (nullCount == 0)
                while (i < x.Length)
                {
                    Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(TWidenerX.Widen(Unsafe.Add(ref xRef, i)),
                                                                     TWidenerY.Widen(Unsafe.Add(ref yRef, i)));
                    i++;
                }
            else
                while (i < x.Length)
                {
                    if (BitUtility.GetBit(resultValidityBitmap, i))
                    {
                        Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(TWidenerX.Widen(Unsafe.Add(ref xRef, i)),
                                                                         TWidenerY.Widen(Unsafe.Add(ref yRef, i)));
                    }
                    i++;
                }
        }
        #endregion
    }
}
