using Apache.Arrow;
using Apache.Arrow.Types;
using Gimpo.ComputeFunctions.Computation.Converters;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Gimpo.ComputeFunctions.Computation.Executors
{
    internal static class BinaryOperatorExecutor
    {
        public static IArrowArray Execute<TBinaryOperator>(Apache.Arrow.Array x, Apache.Arrow.Array y, byte[] resultValidityBitmap)
            where TBinaryOperator : struct, IBinaryOperator
        {
            var length = x.Length;
            var nullCount = resultValidityBitmap.Length == 0 ? 0 : length - BitUtility.CountBits(resultValidityBitmap, 0, length);

            switch (x.Data.DataType.TypeId)
            {
                #region Double
                case ArrowTypeId.Double:
                    {
                        var result = GC.AllocateUninitializedArray<double>(length);
                        var xValues = ((PrimitiveArray<double>)x).Values;

                        switch (y.Data.DataType.TypeId)
                        {
                            case ArrowTypeId.Double:
                                {
                                    InvokeOperator<TBinaryOperator, double>(xValues, ((PrimitiveArray<double>)y).Values, result, resultValidityBitmap, nullCount);
                                    break;
                                }
                            case ArrowTypeId.Float:
                                {
                                    InvokeOperatorWithWidening<TBinaryOperator, double, float, ToDoubleConverter>(xValues, ((PrimitiveArray<float>)y).Values, result, resultValidityBitmap, nullCount);
                                    break;
                                }
                            case ArrowTypeId.HalfFloat:
                                {
                                    InvokeOperator<TBinaryOperator, double, Half, ToDoubleConverter>(xValues, ((PrimitiveArray<Half>)y).Values, result, resultValidityBitmap, nullCount);
                                    break;
                                }
                            case ArrowTypeId.Int64:
                                {
                                    InvokeOperator<TBinaryOperator, double, long, ToDoubleConverter>(xValues, ((PrimitiveArray<long>)y).Values, result, resultValidityBitmap, nullCount);
                                    break;
                                }
                            case ArrowTypeId.UInt64:
                                {
                                    InvokeOperator<TBinaryOperator, double, ulong, ToDoubleConverter>(xValues, ((PrimitiveArray<ulong>)y).Values, result, resultValidityBitmap, nullCount);
                                    break;
                                }
                            case ArrowTypeId.Int32:
                                {
                                    InvokeOperatorWithWidening<TBinaryOperator, double, int, ToDoubleConverter>(xValues, ((PrimitiveArray<int>)y).Values, result, resultValidityBitmap, nullCount);
                                    break;
                                }
                            case ArrowTypeId.UInt32:
                                {
                                    InvokeOperatorWithWidening<TBinaryOperator, double, uint, ToDoubleConverter>(xValues, ((PrimitiveArray<uint>)y).Values, result, resultValidityBitmap, nullCount);
                                    break;
                                }
                            case ArrowTypeId.Int16:
                                {
                                    InvokeOperator<TBinaryOperator, double, short, ToDoubleConverter>(xValues, ((PrimitiveArray<short>)y).Values, result, resultValidityBitmap, nullCount);
                                    break;
                                }
                            case ArrowTypeId.UInt16:
                                {
                                    InvokeOperator<TBinaryOperator, double, ushort, ToDoubleConverter>(xValues, ((PrimitiveArray<ushort>)y).Values, result, resultValidityBitmap, nullCount);
                                    break;
                                }
                            case ArrowTypeId.Int8:
                                {
                                    InvokeOperator<TBinaryOperator, double, sbyte, ToDoubleConverter>(xValues, ((PrimitiveArray<sbyte>)y).Values, result, resultValidityBitmap, nullCount);
                                    break;
                                }
                            case ArrowTypeId.UInt8:
                                {
                                    InvokeOperator<TBinaryOperator, double, byte, ToDoubleConverter>(xValues, ((PrimitiveArray<byte>)y).Values, result, resultValidityBitmap, nullCount);
                                    break;
                                }
                            default: throw new NotSupportedException();
                        }

                        return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                    }
                #endregion

                #region Float
                case ArrowTypeId.Float:
                    {
                        var xValues = ((PrimitiveArray<float>)x).Values;

                        if (y.Data.DataType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, double, float, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, result, resultValidityBitmap, nullCount);

                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.Float:
                                    {
                                        InvokeOperator<TBinaryOperator, float>(xValues, ((PrimitiveArray<float>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.HalfFloat:
                                    {
                                        InvokeOperator<TBinaryOperator, float, Half, ToFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int64:
                                    {
                                        InvokeOperator<TBinaryOperator, float, long, ToFloatConverter>(xValues, ((PrimitiveArray<long>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt64:
                                    {
                                        InvokeOperator<TBinaryOperator, float, ulong, ToFloatConverter>(xValues, ((PrimitiveArray<ulong>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int32:
                                    {
                                        InvokeOperator<TBinaryOperator, float, int, ToFloatConverter>(xValues, ((PrimitiveArray<int>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt32:
                                    {
                                        InvokeOperator<TBinaryOperator, float, uint, ToFloatConverter>(xValues, ((PrimitiveArray<uint>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int16:
                                    {
                                        InvokeOperatorWithWidening<TBinaryOperator, float, short, ToFloatConverter>(xValues, ((PrimitiveArray<short>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt16:
                                    {
                                        InvokeOperatorWithWidening<TBinaryOperator, float, ushort, ToFloatConverter>(xValues, ((PrimitiveArray<ushort>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int8:
                                    {
                                        InvokeOperator<TBinaryOperator, float, sbyte, ToFloatConverter>(xValues, ((PrimitiveArray<sbyte>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        InvokeOperator<TBinaryOperator, float, byte, ToFloatConverter>(xValues, ((PrimitiveArray<byte>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                    }
                #endregion

                #region HalfFloat
                case ArrowTypeId.HalfFloat:
                    {
                        var xValues = ((PrimitiveArray<Half>)x).Values;

                        if (y.Data.DataType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            InvokeOperator<TBinaryOperator, double, Half, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, result, resultValidityBitmap, nullCount);

                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            InvokeOperator<TBinaryOperator, float, Half, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, result, resultValidityBitmap, nullCount);

                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.HalfFloat:
                                    {
                                        InvokeOperator<TBinaryOperator, Half>(xValues, ((PrimitiveArray<Half>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int64:
                                    {
                                        InvokeOperator<TBinaryOperator, Half, long, ToHalfFloatConverter>(xValues, ((PrimitiveArray<long>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt64:
                                    {
                                        InvokeOperator<TBinaryOperator, Half, ulong, ToHalfFloatConverter>(xValues, ((PrimitiveArray<ulong>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int32:
                                    {
                                        InvokeOperator<TBinaryOperator, Half, int, ToHalfFloatConverter>(xValues, ((PrimitiveArray<int>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt32:
                                    {
                                        InvokeOperator<TBinaryOperator, Half, uint, ToHalfFloatConverter>(xValues, ((PrimitiveArray<uint>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int16:
                                    {
                                        InvokeOperator<TBinaryOperator, Half, short, ToHalfFloatConverter>(xValues, ((PrimitiveArray<short>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt16:
                                    {
                                        InvokeOperator<TBinaryOperator, Half, ushort, ToHalfFloatConverter>(xValues, ((PrimitiveArray<ushort>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int8:
                                    {
                                        InvokeOperator<TBinaryOperator, Half, sbyte, ToHalfFloatConverter>(xValues, ((PrimitiveArray<sbyte>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        InvokeOperator<TBinaryOperator, Half, byte, ToHalfFloatConverter>(xValues, ((PrimitiveArray<byte>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                default: throw new NotSupportedException();

                            }

                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);

                        }
                    }
                #endregion

                #region Int64
                case ArrowTypeId.Int64:
                    {
                        var xValues = ((PrimitiveArray<long>)x).Values;

                        if (y.Data.DataType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            InvokeOperator<TBinaryOperator, double, long, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            InvokeOperator<TBinaryOperator, float, long, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            InvokeOperator<TBinaryOperator, Half, long, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.Int64:
                                    {
                                        InvokeOperator<TBinaryOperator, long>(xValues, ((PrimitiveArray<long>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int32:
                                    {
                                        InvokeOperatorWithWidening<TBinaryOperator, long, int, ToInt64Converter>(xValues, ((PrimitiveArray<int>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt32:
                                    {
                                        InvokeOperatorWithWidening<TBinaryOperator, long, uint, ToInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int16:
                                    {
                                        InvokeOperator<TBinaryOperator, long, short, ToInt64Converter>(xValues, ((PrimitiveArray<short>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt16:
                                    {
                                        InvokeOperator<TBinaryOperator, long, ushort, ToInt64Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int8:
                                    {
                                        InvokeOperator<TBinaryOperator, long, sbyte, ToInt64Converter>(xValues, ((PrimitiveArray<sbyte>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        InvokeOperator<TBinaryOperator, long, byte, ToInt64Converter>(xValues, ((PrimitiveArray<byte>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                    }
                #endregion

                #region UInt64
                case ArrowTypeId.UInt64:
                    {
                        var xValues = ((PrimitiveArray<ulong>)x).Values;

                        if (y.Data.DataType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            InvokeOperator<TBinaryOperator, double, ulong, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            InvokeOperator<TBinaryOperator, float, ulong, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            InvokeOperator<TBinaryOperator, Half, ulong, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<ulong>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.UInt64:
                                    {
                                        InvokeOperator<TBinaryOperator, ulong>(xValues, ((PrimitiveArray<ulong>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt32:
                                    {
                                        InvokeOperatorWithWidening<TBinaryOperator, ulong, uint, ToUInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt16:
                                    {
                                        InvokeOperator<TBinaryOperator, ulong, ushort, ToUInt64Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        InvokeOperator<TBinaryOperator, ulong, byte, ToUInt64Converter>(xValues, ((PrimitiveArray<byte>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                    }
                #endregion

                #region Int32
                case ArrowTypeId.Int32:
                    {
                        var xValues = ((PrimitiveArray<int>)x).Values;

                        if (y.Data.DataType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, double, int, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            InvokeOperator<TBinaryOperator, float, int, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            InvokeOperator<TBinaryOperator, Half, int, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, long, int, ToInt64Converter>(xValues, ((PrimitiveArray<long>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, long, int, uint, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.Int32:
                                    {
                                        InvokeOperator<TBinaryOperator, int>(xValues, ((PrimitiveArray<int>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int16:
                                    {
                                        InvokeOperatorWithWidening<TBinaryOperator, int, short, ToInt32Converter>(xValues, ((PrimitiveArray<short>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt16:
                                    {
                                        InvokeOperatorWithWidening<TBinaryOperator, int, ushort, ToInt32Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int8:
                                    {
                                        InvokeOperator<TBinaryOperator, int, sbyte, ToInt32Converter>(xValues, ((PrimitiveArray<sbyte>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        InvokeOperator<TBinaryOperator, int, byte, ToInt32Converter>(xValues, ((PrimitiveArray<byte>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                    }
                #endregion

                #region UInt32
                case ArrowTypeId.UInt32:
                    {
                        var xValues = ((PrimitiveArray<uint>)x).Values;
                        if (y.Data.DataType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, double, uint, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            InvokeOperator<TBinaryOperator, float, uint, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            InvokeOperator<TBinaryOperator, Half, uint, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, long, uint, ToInt64Converter>(xValues, ((PrimitiveArray<long>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.UInt64)
                        {
                            var result = GC.AllocateUninitializedArray<ulong>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, ulong, uint, ToUInt64Converter>(xValues, ((PrimitiveArray<ulong>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, long, uint, int, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<int>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperator<TBinaryOperator, long, uint, short, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<short>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperator<TBinaryOperator, long, uint, sbyte, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<sbyte>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<uint>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.UInt32:
                                    {
                                        InvokeOperator<TBinaryOperator, uint>(xValues, ((PrimitiveArray<uint>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt16:
                                    {
                                        InvokeOperatorWithWidening<TBinaryOperator, uint, ushort, ToUInt32Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        InvokeOperator<TBinaryOperator, uint, byte, ToUInt32Converter>(xValues, ((PrimitiveArray<byte>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                    }
                #endregion

                #region Int16
                case ArrowTypeId.Int16:
                    {
                        var xValues = ((PrimitiveArray<short>)x).Values;

                        if (y.Data.DataType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            InvokeOperator<TBinaryOperator, double, short, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, float, short, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            InvokeOperator<TBinaryOperator, Half, short, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperator<TBinaryOperator, long, short, ToInt64Converter>(xValues, ((PrimitiveArray<long>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, int, short, ToInt32Converter>(xValues, ((PrimitiveArray<int>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperator<TBinaryOperator, long, short, uint, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.UInt16)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, int, short, ushort, ToInt32Converter, ToInt32Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<short>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.Int16:
                                    {
                                        InvokeOperator<TBinaryOperator, short>(xValues, ((PrimitiveArray<short>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.Int8:
                                    {
                                        InvokeOperatorWithWidening<TBinaryOperator, short, sbyte, ToInt16Converter>(xValues, ((PrimitiveArray<sbyte>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        InvokeOperatorWithWidening<TBinaryOperator, short, byte, ToInt16Converter>(xValues, ((PrimitiveArray<byte>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                    }
                #endregion

                #region UInt16
                case ArrowTypeId.UInt16:
                    {
                        var xValues = ((PrimitiveArray<ushort>)x).Values;

                        if (y.Data.DataType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            InvokeOperator<TBinaryOperator, double, ushort, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, float, ushort, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            InvokeOperator<TBinaryOperator, Half, ushort, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperator<TBinaryOperator, long, ushort, ToInt64Converter>(xValues, ((PrimitiveArray<long>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, int, ushort, ToInt32Converter>(xValues, ((PrimitiveArray<int>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperator<TBinaryOperator, long, ushort, uint, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, int, ushort, short, ToInt32Converter, ToInt32Converter>(xValues, ((PrimitiveArray<short>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            InvokeOperator<TBinaryOperator, int, ushort, byte, ToInt32Converter, ToInt32Converter>(xValues, ((PrimitiveArray<byte>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<ushort>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.UInt16:
                                    {
                                        InvokeOperator<TBinaryOperator, ushort>(xValues, ((PrimitiveArray<ushort>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        InvokeOperatorWithWidening<TBinaryOperator, ushort, byte, ToUInt16Converter>(xValues, ((PrimitiveArray<byte>)y).Values, result, resultValidityBitmap, nullCount);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                    }
                #endregion

                #region Int8
                case ArrowTypeId.Int8:
                    {
                        var xValues = ((PrimitiveArray<sbyte>)x).Values;

                        if (y.Data.DataType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            InvokeOperator<TBinaryOperator, double, sbyte, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            InvokeOperator<TBinaryOperator, float, sbyte, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            InvokeOperator<TBinaryOperator, Half, sbyte, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperator<TBinaryOperator, long, sbyte, ToInt64Converter>(xValues, ((PrimitiveArray<long>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            InvokeOperator<TBinaryOperator, int, sbyte, ToInt32Converter>(xValues, ((PrimitiveArray<int>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperator<TBinaryOperator, long, sbyte, uint, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<short>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, short, sbyte, ToInt16Converter>(xValues, ((PrimitiveArray<short>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.UInt16)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            InvokeOperator<TBinaryOperator, int, sbyte, ushort, ToInt32Converter, ToInt32Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.UInt8)
                        {
                            var result = GC.AllocateUninitializedArray<short>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, short, sbyte, byte, ToInt16Converter, ToInt16Converter>(xValues, ((PrimitiveArray<byte>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<sbyte>(length);
                            InvokeOperator<TBinaryOperator, sbyte>(xValues, ((PrimitiveArray<sbyte>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        throw new NotSupportedException();
                    }
                #endregion

                #region UInt8
                case ArrowTypeId.UInt8:
                    {
                        var xValues = ((PrimitiveArray<byte>)x).Values;

                        if (y.Data.DataType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            InvokeOperator<TBinaryOperator, double, byte, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            InvokeOperator<TBinaryOperator, float, byte, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            InvokeOperator<TBinaryOperator, Half, byte, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperator<TBinaryOperator, long, byte, ToInt64Converter>(xValues, ((PrimitiveArray<long>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            InvokeOperator<TBinaryOperator, int, byte, ToInt32Converter>(xValues, ((PrimitiveArray<int>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            InvokeOperator<TBinaryOperator, long, byte, uint, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<short>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, short, byte, ToInt16Converter>(xValues, ((PrimitiveArray<short>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.UInt16)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            InvokeOperator<TBinaryOperator, int, byte, ushort, ToInt32Converter, ToInt32Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<short>(length);
                            InvokeOperatorWithWidening<TBinaryOperator, short, byte, sbyte, ToInt16Converter, ToInt16Converter>(xValues, ((PrimitiveArray<sbyte>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        else if (y.Data.DataType.TypeId == ArrowTypeId.UInt8)
                        {
                            var result = GC.AllocateUninitializedArray<byte>(length);
                            InvokeOperator<TBinaryOperator, byte>(xValues, ((PrimitiveArray<byte>)y).Values, result, resultValidityBitmap, nullCount);
                            return ArrowArrayFactory.BuildArray(result, resultValidityBitmap);
                        }
                        throw new NotSupportedException();
                    }
                    
                #endregion

                throw new NotSupportedException();
            }

            throw new NotSupportedException();
        }

        private static void InvokeOperator<TBinaryOperator, TResult>(ReadOnlySpan<TResult> x, ReadOnlySpan<TResult> y, Span<TResult> destination, ReadOnlySpan<byte> resultValidityBitmap, int nullCount)
            where TBinaryOperator : struct, IBinaryOperator
            where TResult : unmanaged, INumber<TResult>
        {
            ref var xRef = ref MemoryMarshal.GetReference(x);
            ref var yRef = ref MemoryMarshal.GetReference(y);
            ref var dRef = ref MemoryMarshal.GetReference(destination);
            int i = 0;

            if (TBinaryOperator.CanVectorize && (TBinaryOperator.CanRightArgumentBeZero || nullCount == 0))
            {
#if NET8_0_OR_GREATER
                if (Vector512.IsHardwareAccelerated && Vector512<TResult>.IsSupported)
                {
                    int vectorSize = Vector512<TResult>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            TBinaryOperator.Invoke(Vector512.LoadUnsafe(ref xRef, (uint)i),
                                                   Vector512.LoadUnsafe(ref yRef, (uint)i))
                                .StoreUnsafe(ref dRef, (uint)i);

                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            TBinaryOperator.Invoke(Vector512.LoadUnsafe(ref xRef, lastVectorIndex),
                                                   Vector512.LoadUnsafe(ref yRef, lastVectorIndex))
                                .StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }
#endif

                if (Vector256.IsHardwareAccelerated && Vector256<TResult>.IsSupported)
                {
                    int vectorSize = Vector256<TResult>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            TBinaryOperator.Invoke(Vector256.LoadUnsafe(ref xRef, (uint)i),
                                                   Vector256.LoadUnsafe(ref yRef, (uint)i))
                                .StoreUnsafe(ref dRef, (uint)i);

                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            TBinaryOperator.Invoke(Vector256.LoadUnsafe(ref xRef, lastVectorIndex),
                                                   Vector256.LoadUnsafe(ref yRef, lastVectorIndex))
                                .StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }

                if (Vector128.IsHardwareAccelerated && Vector128<TResult>.IsSupported)
                {
                    int vectorSize = Vector128<TResult>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            TBinaryOperator.Invoke(Vector128.LoadUnsafe(ref xRef, (uint)i),
                                                   Vector128.LoadUnsafe(ref yRef, (uint)i))
                                .StoreUnsafe(ref dRef, (uint)i);

                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            TBinaryOperator.Invoke(Vector128.LoadUnsafe(ref xRef, lastVectorIndex),
                                                   Vector128.LoadUnsafe(ref yRef, lastVectorIndex))
                                .StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }
            }

            if (nullCount == 0)
                while (i < x.Length)
                {
                    Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(Unsafe.Add(ref xRef, i),
                                                                     Unsafe.Add(ref yRef, i));
                    i++;
                }
            else
                while (i < x.Length)
                {
                    if (BitUtility.GetBit(resultValidityBitmap, i))
                    {
                        Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(Unsafe.Add(ref xRef, i),
                                                                         Unsafe.Add(ref yRef, i));
                    }
                    i++;
                }
        }

        private static void InvokeOperator<TBinaryOperator, TResult, T, TConverter>(ReadOnlySpan<TResult> x, ReadOnlySpan<T> y, Span<TResult> destination, ReadOnlySpan<byte> resultValidityBitmap, int nullCount)
            where TBinaryOperator : struct, IBinaryOperator
            where TResult : unmanaged, INumber<TResult>
            where T : unmanaged, INumber<T>
            where TConverter : unmanaged, IConverter<TResult, T>
        {
            ref var xRef = ref MemoryMarshal.GetReference(x);
            ref var yRef = ref MemoryMarshal.GetReference(y);
            ref var dRef = ref MemoryMarshal.GetReference(destination);
            int i = 0;

            if (TBinaryOperator.CanVectorize && TConverter.CanVectorize && (TBinaryOperator.CanRightArgumentBeZero || nullCount == 0))
            {
#if NET8_0_OR_GREATER
                if (Vector512.IsHardwareAccelerated && Vector512<TResult>.IsSupported)
                {
                    int vectorSize = Vector512<TResult>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            TBinaryOperator.Invoke(Vector512.LoadUnsafe(ref xRef, (uint)i),
                                                   TConverter.Convert(Vector512.LoadUnsafe(ref yRef, (uint)i)))
                                .StoreUnsafe(ref dRef, (uint)i);

                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            TBinaryOperator.Invoke(Vector512.LoadUnsafe(ref xRef, lastVectorIndex),
                                                   TConverter.Convert(Vector512.LoadUnsafe(ref yRef, lastVectorIndex)))
                                .StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }
#endif

                if (Vector256.IsHardwareAccelerated && Vector256<TResult>.IsSupported)
                {
                    int vectorSize = Vector256<TResult>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            TBinaryOperator.Invoke(Vector256.LoadUnsafe(ref xRef, (uint)i),
                                                   TConverter.Convert(Vector256.LoadUnsafe(ref yRef, (uint)i)))
                                .StoreUnsafe(ref dRef, (uint)i);

                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            TBinaryOperator.Invoke(Vector256.LoadUnsafe(ref xRef, lastVectorIndex),
                                                   TConverter.Convert(Vector256.LoadUnsafe(ref yRef, lastVectorIndex)))
                                .StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }

                if (Vector128.IsHardwareAccelerated && Vector128<TResult>.IsSupported)
                {
                    int vectorSize = Vector128<TResult>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            TBinaryOperator.Invoke(Vector128.LoadUnsafe(ref xRef, (uint)i),
                                                   TConverter.Convert(Vector128.LoadUnsafe(ref yRef, (uint)i)))
                                .StoreUnsafe(ref dRef, (uint)i);

                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            TBinaryOperator.Invoke(Vector128.LoadUnsafe(ref xRef, lastVectorIndex),
                                                   TConverter.Convert(Vector128.LoadUnsafe(ref yRef, lastVectorIndex)))
                                .StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }
            }

            if (nullCount == 0)
                while (i < x.Length)
                {
                    Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(Unsafe.Add(ref xRef, i),
                                                                     TConverter.Convert(Unsafe.Add(ref yRef, i)));
                    i++;
                }
            else
                while (i < x.Length)
                {
                    if (BitUtility.GetBit(resultValidityBitmap, i))
                    {
                        Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(Unsafe.Add(ref xRef, i),
                                                                         TConverter.Convert(Unsafe.Add(ref yRef, i)));
                    }
                    i++;
                }
        }

        private static void InvokeOperator<TBinaryOperator, TResult, T, TConverter>(ReadOnlySpan<T> x, ReadOnlySpan<TResult> y, Span<TResult> destination, ReadOnlySpan<byte> resultValidityBitmap, int nullCount)
            where TBinaryOperator : struct, IBinaryOperator
            where TResult : unmanaged, INumber<TResult>
            where T : unmanaged, INumber<T>
            where TConverter : unmanaged, IConverter<TResult, T>
        {
            ref var xRef = ref MemoryMarshal.GetReference(x);
            ref var yRef = ref MemoryMarshal.GetReference(y);
            ref var dRef = ref MemoryMarshal.GetReference(destination);
            int i = 0;

            if (TBinaryOperator.CanVectorize && TConverter.CanVectorize && (TBinaryOperator.CanRightArgumentBeZero || nullCount == 0))
            {
#if NET8_0_OR_GREATER
                if (Vector512.IsHardwareAccelerated && Vector512<TResult>.IsSupported)
                {
                    int vectorSize = Vector512<TResult>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            TBinaryOperator.Invoke(TConverter.Convert(Vector512.LoadUnsafe(ref xRef, (uint)i)),
                                                   Vector512.LoadUnsafe(ref yRef, (uint)i))
                                .StoreUnsafe(ref dRef, (uint)i);

                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            TBinaryOperator.Invoke(TConverter.Convert(Vector512.LoadUnsafe(ref xRef, lastVectorIndex)),
                                                   Vector512.LoadUnsafe(ref yRef, lastVectorIndex))
                                .StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }
#endif

                if (Vector256.IsHardwareAccelerated && Vector256<TResult>.IsSupported)
                {
                    int vectorSize = Vector256<TResult>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            TBinaryOperator.Invoke(TConverter.Convert(Vector256.LoadUnsafe(ref xRef, (uint)i)),
                                                   Vector256.LoadUnsafe(ref yRef, (uint)i))
                                .StoreUnsafe(ref dRef, (uint)i);

                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            TBinaryOperator.Invoke(TConverter.Convert(Vector256.LoadUnsafe(ref xRef, lastVectorIndex)),
                                                   Vector256.LoadUnsafe(ref yRef, lastVectorIndex))
                                .StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }

                if (Vector128.IsHardwareAccelerated && Vector128<TResult>.IsSupported)
                {
                    int vectorSize = Vector128<TResult>.Count;
                    int oneVectorFromEnd = x.Length - vectorSize;
                    if (i <= oneVectorFromEnd)
                    {
                        // Loop handling one vector at a time.
                        do
                        {
                            TBinaryOperator.Invoke(TConverter.Convert(Vector128.LoadUnsafe(ref xRef, (uint)i)),
                                                   Vector128.LoadUnsafe(ref yRef, (uint)i))
                                .StoreUnsafe(ref dRef, (uint)i);

                            i += vectorSize;
                        }
                        while (i <= oneVectorFromEnd);

                        // Handle any remaining elements with a final vector.
                        if (i != x.Length)
                        {
                            uint lastVectorIndex = (uint)oneVectorFromEnd;
                            TBinaryOperator.Invoke(TConverter.Convert(Vector128.LoadUnsafe(ref xRef, lastVectorIndex)),
                                                   Vector128.LoadUnsafe(ref yRef, lastVectorIndex))
                                .StoreUnsafe(ref dRef, lastVectorIndex);
                        }

                        return;
                    }
                }
            }

            if (nullCount == 0)
                while (i < x.Length)
                {
                    Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(TConverter.Convert(Unsafe.Add(ref xRef, i)),
                                                                     Unsafe.Add(ref yRef, i));
                    i++;
                }
            else
                while (i < x.Length)
                {
                    if (BitUtility.GetBit(resultValidityBitmap, i))
                    {
                        Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(TConverter.Convert(Unsafe.Add(ref xRef, i)),
                                                                         Unsafe.Add(ref yRef, i));
                    }
                    i++;
                }
        }

        private static void InvokeOperatorWithWidening<TBinaryOperator, TResult, T, TWidener>(ReadOnlySpan<TResult> x, ReadOnlySpan<T> y, Span<TResult> destination, ReadOnlySpan<byte> resultValidityBitmap, int nullCount)
           where TBinaryOperator : struct, IBinaryOperator
           where TResult : unmanaged, INumber<TResult>
           where T : unmanaged, INumber<T>
           where TWidener : struct, IWidener<TResult, T>
        {
            ref var xRef = ref MemoryMarshal.GetReference(x);
            ref var yRef = ref MemoryMarshal.GetReference(y);
            ref var dRef = ref MemoryMarshal.GetReference(destination);
            int i = 0;

            if (TBinaryOperator.CanVectorize && (TBinaryOperator.CanRightArgumentBeZero || nullCount == 0))
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

        private static void InvokeOperatorWithWidening<TBinaryOperator, TResult, T, TWidener>(ReadOnlySpan<T> x, ReadOnlySpan<TResult> y, Span<TResult> destination, ReadOnlySpan<byte> resultValidityBitmap, int nullCount)
          where TBinaryOperator : struct, IBinaryOperator
          where TResult : unmanaged, INumber<TResult>
          where T : unmanaged, INumber<T>
          where TWidener : struct, IWidener<TResult, T>
        {
            ref var xRef = ref MemoryMarshal.GetReference(x);
            ref var yRef = ref MemoryMarshal.GetReference(y);
            ref var dRef = ref MemoryMarshal.GetReference(destination);
            int i = 0;

            if (TBinaryOperator.CanVectorize && (TBinaryOperator.CanRightArgumentBeZero || nullCount == 0))
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

        private static void InvokeOperatorWithWidening<TBinaryOperator, TResult, TX, TY, TWidenerX, TWidenerY>(ReadOnlySpan<TX> x, ReadOnlySpan<TY> y, Span<TResult> destination, ReadOnlySpan<byte> resultValidityBitmap, int nullCount)
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

            if (TBinaryOperator.CanVectorize && (TBinaryOperator.CanRightArgumentBeZero || nullCount == 0))
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

        private static void InvokeOperator<TBinaryOperator, TResult, TX, TY, TConverterX, TConverterY>(ReadOnlySpan<TX> x, ReadOnlySpan<TY> y, Span<TResult> destination, ReadOnlySpan<byte> resultValidityBitmap, int nullCount)
          where TBinaryOperator : struct, IBinaryOperator
          where TResult : unmanaged, INumber<TResult>
          where TX : unmanaged, INumber<TX>
          where TY : unmanaged, INumber<TY>
          where TConverterX : struct, IConverter<TResult, TX>
          where TConverterY : struct, IConverter<TResult, TY>
        {
            ref var xRef = ref MemoryMarshal.GetReference(x);
            ref var yRef = ref MemoryMarshal.GetReference(y);
            ref var dRef = ref MemoryMarshal.GetReference(destination);
            int i = 0;

            if (nullCount == 0)
                while (i < x.Length)
                {

                    Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(TConverterX.Convert(Unsafe.Add(ref xRef, i)),
                                                                     TConverterY.Convert(Unsafe.Add(ref yRef, i)));
                    i++;
                }
            else
                while (i < x.Length)
                {
                    if (BitUtility.GetBit(resultValidityBitmap, i))
                    {
                        Unsafe.Add(ref dRef, i) = TBinaryOperator.Invoke(TConverterX.Convert(Unsafe.Add(ref xRef, i)),
                                                                         TConverterY.Convert(Unsafe.Add(ref yRef, i)));
                    }
                    i++;
                }
        }
    }
}
