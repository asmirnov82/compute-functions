using Apache.Arrow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions
{
    /// <summary>
    /// Allows to create ArrowArrays from a span of numeric values and a validity bitmap.
    /// </summary>
    internal static class ArrowArrayFactory
    {
        public static IArrowArray BuildArray(ReadOnlySpan<double> values, byte[] validityBitmap, int? nullCount = null)
        {
            return new DoubleArray(new ArrowBuffer.Builder<double>(values.Length).Append(values).Build(),
                            new ArrowBuffer(validityBitmap),
                            values.Length,
                            nullCount ?? CalculateNullCount(validityBitmap, values.Length),
                            0);
        }

        public static IArrowArray BuildArray(ReadOnlySpan<float> values, byte[] validityBitmap, int? nullCount = null)
        {
            return new FloatArray(new ArrowBuffer.Builder<float>(values.Length).Append(values).Build(),
                            new ArrowBuffer(validityBitmap),
                            values.Length,
                            nullCount ?? CalculateNullCount(validityBitmap, values.Length),
                            0);
        }

        public static IArrowArray BuildArray(ReadOnlySpan<Half> values, byte[] validityBitmap, int? nullCount = null)
        {
            return new HalfFloatArray(new ArrowBuffer.Builder<Half>(values.Length).Append(values).Build(),
                            new ArrowBuffer(validityBitmap),
                            values.Length,
                            nullCount ?? CalculateNullCount(validityBitmap, values.Length),
                            0);
        }

        public static IArrowArray BuildArray(ReadOnlySpan<long> values, byte[] validityBitmap, int? nullCount = null)
        {
            return new Int64Array(new ArrowBuffer.Builder<long>(values.Length).Append(values).Build(),
                            new ArrowBuffer(validityBitmap),
                            values.Length,
                            nullCount ?? CalculateNullCount(validityBitmap, values.Length),
                            0);
        }

        public static IArrowArray BuildArray(ReadOnlySpan<ulong> values, byte[] validityBitmap, int? nullCount = null)
        { 
            return new UInt64Array(new ArrowBuffer.Builder<ulong>(values.Length).Append(values).Build(),
                            new ArrowBuffer(validityBitmap),
                            values.Length,
                            nullCount ?? CalculateNullCount(validityBitmap, values.Length),
                            0);
        }

        public static IArrowArray BuildArray(ReadOnlySpan<int> values, byte[] validityBitmap, int? nullCount = null)
        {
            return new Int32Array(new ArrowBuffer.Builder<int>(values.Length).Append(values).Build(),
                            new ArrowBuffer(validityBitmap),
                            values.Length,
                            nullCount ?? CalculateNullCount(validityBitmap, values.Length),
                            0);
        }

        public static IArrowArray BuildArray(ReadOnlySpan<uint> values, byte[] validityBitmap, int? nullCount = null)
        { 
            return new UInt32Array(new ArrowBuffer.Builder<uint>(values.Length).Append(values).Build(),
                            new ArrowBuffer(validityBitmap),
                            values.Length,
                            nullCount ?? CalculateNullCount(validityBitmap, values.Length),
                            0);
        }

        public static IArrowArray BuildArray(ReadOnlySpan<short> values, byte[] validityBitmap, int? nullCount = null)
        {
            return new Int16Array(new ArrowBuffer.Builder<short>(values.Length).Append(values).Build(),
                            new ArrowBuffer(validityBitmap),
                            values.Length,
                            nullCount ?? CalculateNullCount(validityBitmap, values.Length),
                            0);
        }

        public static IArrowArray BuildArray(ReadOnlySpan<ushort> values, byte[] validityBitmap, int? nullCount = null)
        {
            return new UInt16Array(new ArrowBuffer.Builder<ushort>(values.Length).Append(values).Build(),
                            new ArrowBuffer(validityBitmap),
                            values.Length,
                            nullCount ?? CalculateNullCount(validityBitmap, values.Length),
                            0);
        }

        public static IArrowArray BuildArray(ReadOnlySpan<sbyte> values, byte[] validityBitmap, int? nullCount = null)
        {
            return new Int8Array(new ArrowBuffer.Builder<sbyte>(values.Length).Append(values).Build(),
                            new ArrowBuffer(validityBitmap),
                            values.Length,
                            nullCount ?? CalculateNullCount(validityBitmap, values.Length),
                            0);
        }

        public static IArrowArray BuildArray(ReadOnlySpan<byte> values, byte[] validityBitmap, int? nullCount = null)
        {
            return new UInt8Array(new ArrowBuffer.Builder<byte>(values.Length).Append(values).Build(),
                            new ArrowBuffer(validityBitmap),
                            values.Length,
                            nullCount ?? CalculateNullCount(validityBitmap, values.Length),
                            0);
        }

        private static int CalculateNullCount(ReadOnlySpan<byte> validityBitmap, int length)
        {
            return validityBitmap.Length == 0 ? 0 : length - BitUtility.CountBits(validityBitmap, 0, length);
        }
    }
}