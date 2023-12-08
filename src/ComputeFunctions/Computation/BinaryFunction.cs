using System;
using Apache.Arrow;
using Apache.Arrow.Types;
using Gimpo.ComputeFunctions.Computation.Converters;
using Gimpo.ComputeFunctions.Computation.Executors;

namespace Gimpo.ComputeFunctions.Computation
{
    internal abstract class BinaryFunction<TBinaryOperator> : IFunction
        where TBinaryOperator : struct, IBinaryOperator
    {
        private readonly string _name;
        public string Name => _name;

        public int ArgumentCount => 2;
        public bool IsVariableArgumentCount => false;

        public BinaryFunction(string name)
        {
            _name = name;
        }

        public Datum Execute(IReadOnlyList<Datum> args)
        {
            if (args.Count != 2)
                ThrowHelper.ThrowArgument_FunctionIncorrectAmountOfArguments(nameof(args));

            if (args[0].Kind == DatumKind.Array)
            {
                if (args[1].Kind == DatumKind.Array)
                    return new Datum(Execute(args[0].Array, args[1].Array));
                else if (args[1].Kind == DatumKind.Scalar)
                    return new Datum(Execute(args[0].Array, args[1].Scalar));
            }
            else if (args[0].Kind == DatumKind.Scalar)
            {
                if (args[1].Kind == DatumKind.Array)
                    return new Datum(Execute(args[0].Scalar, args[1].Array));
                else if (args[1].Kind == DatumKind.Scalar)
                    throw new NotImplementedException(); //TODO
            }

            throw new NotImplementedException(); //TODO
        }

        public static IArrowArray Execute(IArrowArray arg1, IArrowArray arg2)
        {
            if (arg1.Length != arg2.Length)
                ThrowHelper.ThrowArgument_FunctionArgumentsArrayLengthMismatch();
            
            var length = arg1.Length;

            if (arg1.Data.DataType.TypeId == ArrowTypeId.Null || arg2.Data.DataType.TypeId == ArrowTypeId.Null)
                return new NullArray(length);

            var x = (Apache.Arrow.Array)arg1;
            var y = (Apache.Arrow.Array)arg2;

            //Calculate result validity bitmap
            var nullBitmapX = x.NullBitmapBuffer.Span;
            var nullBitmapY = y.NullBitmapBuffer.Span;

            byte[] resValidityBitmap;

            if (nullBitmapX.Length == 0)
                resValidityBitmap = nullBitmapY.ToArray();
            else if (nullBitmapY.Length == 0)
                resValidityBitmap = nullBitmapX.ToArray();
            else
            {
                var bitmapLength = Math.Min(nullBitmapX.Length, nullBitmapY.Length);
                resValidityBitmap = GC.AllocateUninitializedArray<byte>(nullBitmapX.Length);

                for (int i = 0; i < bitmapLength; i++)
                    resValidityBitmap[i] = (byte)(nullBitmapX[i] & nullBitmapY[i]);
            }
                     
            return Execute(x, y, resValidityBitmap);
        }

        public static IArrowArray Execute(IArrowArray arg1, Scalar arg2)
        {            
            var length = arg1.Length;

            if (arg1.Data.DataType.TypeId == ArrowTypeId.Null || arg2.ValueType.TypeId == ArrowTypeId.Null)
                return new NullArray(length);
                        
            var x = (Apache.Arrow.Array)arg1;
            var y = arg2;

            return Execute(x, y, x.NullBitmapBuffer.Span.ToArray());
        }

        public static IArrowArray Execute(Scalar arg1, IArrowArray arg2)
        {
            var length = arg2.Length;

            if (arg2.Data.DataType.TypeId == ArrowTypeId.Null || arg1.ValueType.TypeId == ArrowTypeId.Null)
                return new NullArray(length);

            if (!arg1.IsNumeric)
                throw new NotSupportedException();

            var x = arg1;
            var y = (Apache.Arrow.Array)arg2;

            return Execute(y, x, y.NullBitmapBuffer.Span.ToArray(), true);
        }

        /*
        public static IArrowArray Execute(Scalar arg1, Scalar arg2)
        {
            
            if (arg1.ValueType.TypeId == ArrowTypeId.Null || arg2.ValueType.TypeId == ArrowTypeId.Null)
                return new NullArray(1);

            if (!arg1.IsNumeric)
                throw new NotSupportedException();

            var x = arg1;
            var y = arg2;

            return BinaryOperatorExecutor.Execute<TBinaryOperator>(x, y);
        }
        */

        #region At least one argument is a Scalar
        private static IArrowArray Execute(Apache.Arrow.Array array, Scalar scalar, byte[] validityBitmap, bool reverseArguments = false)
        {
            if (!scalar.IsNumeric)
                throw new NotSupportedException();

            var length = array.Length;
            var nullCount = validityBitmap.Length == 0 ? 0 : length - BitUtility.CountBits(validityBitmap, 0, length);

            //
            switch (array.Data.DataType.TypeId)
            {
                #region Double
                case ArrowTypeId.Double:
                    {
                        var result = GC.AllocateUninitializedArray<double>(length);
                        var arrayValues = ((PrimitiveArray<double>)array).Values;
                        double scalarValue;
                        switch (scalar.ValueType.TypeId)
                        {
                            case ArrowTypeId.Double:
                                scalarValue = ((NumericScalar<double>)scalar).Value;
                                break;
                            case ArrowTypeId.Float:
                                scalarValue = ((NumericScalar<float>)scalar).Value;
                                break;
                            case ArrowTypeId.HalfFloat:
                                scalarValue = (double)((NumericScalar<Half>)scalar).Value;
                                break;
                            case ArrowTypeId.Int64:
                                scalarValue = ((NumericScalar<long>)scalar).Value;
                                break;
                            case ArrowTypeId.UInt64:
                                scalarValue = ((NumericScalar<ulong>)scalar).Value;
                                break;
                            case ArrowTypeId.Int32:
                                scalarValue = ((NumericScalar<int>)scalar).Value;
                                break;
                            case ArrowTypeId.UInt32:
                                scalarValue = ((NumericScalar<uint>)scalar).Value;
                                break;
                            case ArrowTypeId.Int16:
                                scalarValue = ((NumericScalar<short>)scalar).Value;
                                break;
                            case ArrowTypeId.UInt16:
                                scalarValue = ((NumericScalar<ushort>)scalar).Value;
                                break;
                            case ArrowTypeId.Int8:
                                scalarValue = ((NumericScalar<sbyte>)scalar).Value;
                                break;
                            case ArrowTypeId.UInt8:
                                scalarValue = ((NumericScalar<byte>)scalar).Value;
                                break;
                            default: throw new NotSupportedException();
                        }
                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                        return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);

                    }
                #endregion

                #region Float
                case ArrowTypeId.Float:
                    {
                        var arrayValues = ((PrimitiveArray<float>)array).Values;

                        if (scalar.ValueType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            var scalarValue = ((NumericScalar<double>)scalar).Value;
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, float, ToDoubleConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            float scalarValue;
                            switch (scalar.ValueType.TypeId)
                            {                                
                                case ArrowTypeId.Float:
                                    scalarValue = ((NumericScalar<float>)scalar).Value;
                                    break;
                                case ArrowTypeId.HalfFloat:
                                    scalarValue = (float)((NumericScalar<Half>)scalar).Value;
                                    break;
                                case ArrowTypeId.Int64:
                                    scalarValue = ((NumericScalar<long>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt64:
                                    scalarValue = ((NumericScalar<ulong>)scalar).Value;
                                    break;
                                case ArrowTypeId.Int32:
                                    scalarValue = ((NumericScalar<int>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt32:
                                    scalarValue = ((NumericScalar<uint>)scalar).Value;
                                    break;
                                case ArrowTypeId.Int16:
                                    scalarValue = ((NumericScalar<short>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt16:
                                    scalarValue = ((NumericScalar<ushort>)scalar).Value;
                                    break;
                                case ArrowTypeId.Int8:
                                    scalarValue = ((NumericScalar<sbyte>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt8:
                                    scalarValue = ((NumericScalar<byte>)scalar).Value;
                                    break;
                                default: throw new NotSupportedException();
                            }
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                    }
                #endregion

                #region Half
                case ArrowTypeId.HalfFloat:
                    {
                        var arrayValues = ((PrimitiveArray<Half>)array).Values;

                        if (scalar.ValueType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            var scalarValue = ((NumericScalar<double>)scalar).Value;
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, Half, ToDoubleConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            var scalarValue = ((NumericScalar<float>)scalar).Value;
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, Half, ToFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            Half scalarValue;
                            switch (scalar.ValueType.TypeId)
                            {                                
                                case ArrowTypeId.HalfFloat:
                                    scalarValue = ((NumericScalar<Half>)scalar).Value;
                                    break;
                                
                                default: throw new NotSupportedException();
                            }
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                    }
                #endregion

                #region Int64
                case ArrowTypeId.Int64:
                    {
                        var arrayValues = ((PrimitiveArray<long>)array).Values;

                        if (scalar.ValueType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            var scalarValue = ((NumericScalar<double>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, long, ToDoubleConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            var scalarValue = ((NumericScalar<float>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, long, ToFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            var scalarValue = ((NumericScalar<Half>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, long, ToHalfFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);

                            long scalarValue;

                            switch (scalar.ValueType.TypeId)
                            {
                                case ArrowTypeId.Int64:
                                    scalarValue = ((NumericScalar<long>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt64:
                                    scalarValue = (long)((NumericScalar<ulong>)scalar).Value;
                                    break;
                                case ArrowTypeId.Int32:
                                    scalarValue = ((NumericScalar<int>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt32:
                                    scalarValue = ((NumericScalar<uint>)scalar).Value;
                                    break;
                                case ArrowTypeId.Int16:
                                    scalarValue = ((NumericScalar<short>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt16:
                                    scalarValue = ((NumericScalar<ushort>)scalar).Value;
                                    break;
                                case ArrowTypeId.Int8:
                                    scalarValue = ((NumericScalar<sbyte>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt8:
                                    scalarValue = ((NumericScalar<byte>)scalar).Value;
                                    break;
                                default: throw new NotSupportedException();
                            }

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                    }
                #endregion

                #region UInt64
                case ArrowTypeId.UInt64:
                    {
                        var arrayValues = ((PrimitiveArray<ulong>)array).Values;

                        if (scalar.ValueType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            var scalarValue = ((NumericScalar<double>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, ulong, ToDoubleConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            var scalarValue = ((NumericScalar<float>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, ulong, ToFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            var scalarValue = ((NumericScalar<Half>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, ulong, ToHalfFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<long>)scalar).Value;
                            
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ulong, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<int>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ulong, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<short>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ulong, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<sbyte>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ulong, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<ulong>(length);

                            ulong scalarValue;

                            switch (scalar.ValueType.TypeId)
                            {                                
                                case ArrowTypeId.UInt64:
                                    scalarValue = ((NumericScalar<ulong>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt32:
                                    scalarValue = ((NumericScalar<uint>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt16:
                                    scalarValue = ((NumericScalar<ushort>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt8:
                                    scalarValue = ((NumericScalar<byte>)scalar).Value;
                                    break;
                                default: throw new NotSupportedException();
                            }

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, ulong>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                    }
                #endregion

                #region Int32
                case ArrowTypeId.Int32:
                    {
                        var arrayValues = ((PrimitiveArray<int>)array).Values;

                        if (scalar.ValueType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            var scalarValue = ((NumericScalar<double>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, int, ToDoubleConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            var scalarValue = ((NumericScalar<float>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, int, ToFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            var scalarValue = ((NumericScalar<Half>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, int, ToHalfFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<long>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, int, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = (long)((NumericScalar<ulong>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, int, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = (long)((NumericScalar<uint>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, int, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);

                            int scalarValue;

                            switch (scalar.ValueType.TypeId)
                            {                                
                                case ArrowTypeId.Int32:
                                    scalarValue = ((NumericScalar<int>)scalar).Value;
                                    break;
                                case ArrowTypeId.Int16:
                                    scalarValue = ((NumericScalar<short>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt16:
                                    scalarValue = ((NumericScalar<ushort>)scalar).Value;
                                    break;
                                case ArrowTypeId.Int8:
                                    scalarValue = ((NumericScalar<sbyte>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt8:
                                    scalarValue = ((NumericScalar<byte>)scalar).Value;
                                    break;
                                default: throw new NotSupportedException();
                            }

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                    }
                #endregion

                #region UInt32
                case ArrowTypeId.UInt32:
                    {
                        var arrayValues = ((PrimitiveArray<uint>)array).Values;

                        if (scalar.ValueType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            var scalarValue = ((NumericScalar<double>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, uint, ToDoubleConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            var scalarValue = ((NumericScalar<float>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, uint, ToFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            var scalarValue = ((NumericScalar<Half>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, uint, ToHalfFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<long>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, uint, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<int>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, uint, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<short>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, uint, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<sbyte>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, uint, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt64)
                        {
                            var result = GC.AllocateUninitializedArray<ulong>(length);
                            var scalarValue = ((NumericScalar<ulong>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, ulong, uint, ToUInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }    
                        else
                        {
                            var result = GC.AllocateUninitializedArray<uint>(length);
                            uint scalarValue;

                            switch (scalar.ValueType.TypeId)
                            {
                                case ArrowTypeId.UInt32:
                                    scalarValue = ((NumericScalar<uint>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt16:
                                    scalarValue = ((NumericScalar<ushort>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt8:
                                    scalarValue = ((NumericScalar<byte>)scalar).Value;
                                    break;
                                default: throw new NotSupportedException();
                            }

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, uint>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                    }
                #endregion

                #region Int16
                case ArrowTypeId.Int16:
                    {
                        var arrayValues = ((PrimitiveArray<short>)array).Values;

                        if (scalar.ValueType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            var scalarValue = ((NumericScalar<double>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, short, ToDoubleConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            var scalarValue = ((NumericScalar<float>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, short, ToFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            var scalarValue = ((NumericScalar<Half>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, short, ToHalfFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<long>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, short, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = (long)((NumericScalar<ulong>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, short, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            var scalarValue = ((NumericScalar<int>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, short, ToInt32Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = (long)((NumericScalar<uint>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, short, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt16)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            var scalarValue = (int)((NumericScalar<ushort>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, short, ToInt32Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<short>(length);
                            short scalarValue;

                            switch (scalar.ValueType.TypeId)
                            {
                                case ArrowTypeId.Int16:
                                    scalarValue = ((NumericScalar<short>)scalar).Value;
                                    break;
                                case ArrowTypeId.Int8:
                                    scalarValue = ((NumericScalar<sbyte>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt8:
                                    scalarValue = ((NumericScalar<byte>)scalar).Value;
                                    break;
                                default: throw new NotSupportedException();
                            }

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, short>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                    }
                #endregion

                #region UInt16
                case ArrowTypeId.UInt16:
                    {
                        var arrayValues = ((PrimitiveArray<ushort>)array).Values;

                        if (scalar.ValueType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            var scalarValue = ((NumericScalar<double>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, ushort, ToDoubleConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            var scalarValue = ((NumericScalar<float>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, ushort, ToFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            var scalarValue = ((NumericScalar<Half>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, ushort, ToHalfFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<long>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ushort, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<int>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ushort, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            var scalarValue = ((NumericScalar<short>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, ushort, ToInt32Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            var scalarValue = ((NumericScalar<sbyte>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, ushort, ToInt32Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt64)
                        {
                            var result = GC.AllocateUninitializedArray<ulong>(length);
                            var scalarValue = ((NumericScalar<ulong>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, ulong, ushort, ToUInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<uint>(length);
                            var scalarValue = ((NumericScalar<uint>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, uint, ushort, ToUInt32Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<ushort>(length);
                            ushort scalarValue;

                            switch (scalar.ValueType.TypeId)
                            {                                
                                case ArrowTypeId.UInt16:
                                    scalarValue = ((NumericScalar<ushort>)scalar).Value;
                                    break;
                                case ArrowTypeId.UInt8:
                                    scalarValue = ((NumericScalar<byte>)scalar).Value;
                                    break;
                                default: throw new NotSupportedException();
                            }

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, ushort>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                    }
                #endregion

                #region Int8
                case ArrowTypeId.Int8:
                    {
                        var arrayValues = ((PrimitiveArray<sbyte>)array).Values;

                        if (scalar.ValueType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            var scalarValue = ((NumericScalar<double>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, sbyte, ToDoubleConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            var scalarValue = ((NumericScalar<float>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, sbyte, ToFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            var scalarValue = ((NumericScalar<Half>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, sbyte, ToHalfFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<long>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, sbyte, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = (long)((NumericScalar<ulong>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, sbyte, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            var scalarValue = ((NumericScalar<int>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, sbyte, ToInt32Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = (long)((NumericScalar<uint>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, sbyte, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt16)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            var scalarValue = (int)((NumericScalar<ushort>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, sbyte, ToInt32Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<short>(length);
                            var scalarValue = ((NumericScalar<short>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, short, sbyte, ToInt16Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<sbyte>(length);
                            var scalarValue = ((NumericScalar<sbyte>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, sbyte>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt8)
                        {
                            var result = GC.AllocateUninitializedArray<short>(length);
                            var scalarValue = ((NumericScalar<short>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, short, sbyte, ToInt16Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }

                        throw new NotSupportedException();
                    }
                #endregion

                #region UInt8
                case ArrowTypeId.UInt8:
                    {
                        var arrayValues = ((PrimitiveArray<byte>)array).Values;

                        if (scalar.ValueType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            var scalarValue = ((NumericScalar<double>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, byte, ToDoubleConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            var scalarValue = ((NumericScalar<float>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, byte, ToFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            var scalarValue = ((NumericScalar<Half>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, byte, ToHalfFloatConverter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<long>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, byte, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            var scalarValue = ((NumericScalar<int>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, byte, ToInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            var scalarValue = ((NumericScalar<short>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, byte, ToInt32Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            var scalarValue = ((NumericScalar<sbyte>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, byte, ToInt32Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt64)
                        {
                            var result = GC.AllocateUninitializedArray<ulong>(length);
                            var scalarValue = ((NumericScalar<ulong>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, ulong, byte, ToUInt64Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<uint>(length);
                            var scalarValue = ((NumericScalar<uint>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, uint, byte, ToUInt32Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt16)
                        {
                            var result = GC.AllocateUninitializedArray<ushort>(length);
                            var scalarValue = ((NumericScalar<ushort>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, ushort, byte, ToUInt16Converter>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (scalar.ValueType.TypeId == ArrowTypeId.UInt8)
                        {
                            var result = GC.AllocateUninitializedArray<byte>(length);
                            var scalarValue = ((NumericScalar<byte>)scalar).Value;

                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, byte>(arrayValues, scalarValue, validityBitmap, nullCount, result, reverseArguments);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }

                        throw new NotSupportedException();
                    }
                #endregion

                default: throw new NotSupportedException();
            }

            throw new NotSupportedException();
        }
        #endregion

        #region Both arguments are arrays
        private static IArrowArray Execute(Apache.Arrow.Array x, Apache.Arrow.Array y, byte[] validityBitmap)
        {
            var length = x.Length;
            var nullCount = validityBitmap.Length == 0 ? 0 : length - BitUtility.CountBits(validityBitmap, 0, length);

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
                                    BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double>(xValues, ((PrimitiveArray<double>)y).Values, validityBitmap, nullCount, result);
                                    break;
                                }
                            case ArrowTypeId.Float:
                                {
                                    BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, double, float, ToDoubleConverter>(xValues, ((PrimitiveArray<float>)y).Values, validityBitmap, nullCount, result);
                                    break;
                                }
                            case ArrowTypeId.HalfFloat:
                                {
                                    BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, Half, ToDoubleConverter>(xValues, ((PrimitiveArray<Half>)y).Values, validityBitmap, nullCount, result);
                                    break;
                                }
                            case ArrowTypeId.Int64:
                                {
                                    BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, long, ToDoubleConverter>(xValues, ((PrimitiveArray<long>)y).Values, validityBitmap, nullCount, result);
                                    break;
                                }
                            case ArrowTypeId.UInt64:
                                {
                                    BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, ulong, ToDoubleConverter>(xValues, ((PrimitiveArray<ulong>)y).Values, validityBitmap, nullCount, result);
                                    break;
                                }
                            case ArrowTypeId.Int32:
                                {
                                    BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, double, int, ToDoubleConverter>(xValues, ((PrimitiveArray<int>)y).Values, validityBitmap, nullCount, result);
                                    break;
                                }
                            case ArrowTypeId.UInt32:
                                {
                                    BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, double, uint, ToDoubleConverter>(xValues, ((PrimitiveArray<uint>)y).Values, validityBitmap, nullCount, result);
                                    break;
                                }
                            case ArrowTypeId.Int16:
                                {
                                    BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, short, ToDoubleConverter>(xValues, ((PrimitiveArray<short>)y).Values, validityBitmap, nullCount, result);
                                    break;
                                }
                            case ArrowTypeId.UInt16:
                                {
                                    BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, ushort, ToDoubleConverter>(xValues, ((PrimitiveArray<ushort>)y).Values, validityBitmap, nullCount, result);
                                    break;
                                }
                            case ArrowTypeId.Int8:
                                {
                                    BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, sbyte, ToDoubleConverter>(xValues, ((PrimitiveArray<sbyte>)y).Values, validityBitmap, nullCount, result);
                                    break;
                                }
                            case ArrowTypeId.UInt8:
                                {
                                    BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, byte, ToDoubleConverter>(xValues, ((PrimitiveArray<byte>)y).Values, validityBitmap, nullCount, result);
                                    break;
                                }
                            default: throw new NotSupportedException();
                        }

                        return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                    }
                #endregion

                #region Float
                case ArrowTypeId.Float:
                    {
                        var xValues = ((PrimitiveArray<float>)x).Values;

                        if (y.Data.DataType.TypeId == ArrowTypeId.Double)
                        {
                            var result = GC.AllocateUninitializedArray<double>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, double, float, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, validityBitmap, nullCount, result);

                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.Float:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float>(xValues, ((PrimitiveArray<float>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.HalfFloat:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, Half, ToFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int64:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, long, ToFloatConverter>(xValues, ((PrimitiveArray<long>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt64:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, ulong, ToFloatConverter>(xValues, ((PrimitiveArray<ulong>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int32:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, int, ToFloatConverter>(xValues, ((PrimitiveArray<int>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt32:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, uint, ToFloatConverter>(xValues, ((PrimitiveArray<uint>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int16:
                                    {
                                        BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, float, short, ToFloatConverter>(xValues, ((PrimitiveArray<short>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt16:
                                    {
                                        BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, float, ushort, ToFloatConverter>(xValues, ((PrimitiveArray<ushort>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int8:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, sbyte, ToFloatConverter>(xValues, ((PrimitiveArray<sbyte>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, byte, ToFloatConverter>(xValues, ((PrimitiveArray<byte>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
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
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, Half, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, validityBitmap, nullCount, result);

                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, Half, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, validityBitmap, nullCount, result);

                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.HalfFloat:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half>(xValues, ((PrimitiveArray<Half>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int64:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, long, ToHalfFloatConverter>(xValues, ((PrimitiveArray<long>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt64:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, ulong, ToHalfFloatConverter>(xValues, ((PrimitiveArray<ulong>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int32:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, int, ToHalfFloatConverter>(xValues, ((PrimitiveArray<int>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt32:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, uint, ToHalfFloatConverter>(xValues, ((PrimitiveArray<uint>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int16:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, short, ToHalfFloatConverter>(xValues, ((PrimitiveArray<short>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt16:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, ushort, ToHalfFloatConverter>(xValues, ((PrimitiveArray<ushort>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int8:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, sbyte, ToHalfFloatConverter>(xValues, ((PrimitiveArray<sbyte>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, byte, ToHalfFloatConverter>(xValues, ((PrimitiveArray<byte>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                default: throw new NotSupportedException();

                            }

                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);

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
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, long, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, long, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, long, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.Int64:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long>(xValues, ((PrimitiveArray<long>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt64:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ulong, ToInt64Converter>(xValues, ((PrimitiveArray<ulong>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int32:
                                    {
                                        BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, long, int, ToInt64Converter>(xValues, ((PrimitiveArray<int>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt32:
                                    {
                                        BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, long, uint, ToInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int16:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, short, ToInt64Converter>(xValues, ((PrimitiveArray<short>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt16:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ushort, ToInt64Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int8:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, sbyte, ToInt64Converter>(xValues, ((PrimitiveArray<sbyte>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, byte, ToInt64Converter>(xValues, ((PrimitiveArray<byte>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
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
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, ulong, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, ulong, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, ulong, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ulong, ToInt64Converter>(xValues, ((PrimitiveArray<long>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ulong, int, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<int>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ulong, short, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<short>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ulong, sbyte, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<sbyte>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<ulong>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.UInt64:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, ulong>(xValues, ((PrimitiveArray<ulong>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt32:
                                    {
                                        BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, ulong, uint, ToUInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt16:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, ulong, ushort, ToUInt64Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, ulong, byte, ToUInt64Converter>(xValues, ((PrimitiveArray<byte>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
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
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, double, int, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, int, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, int, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, long, int, ToInt64Converter>(xValues, ((PrimitiveArray<long>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, long, int, uint, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.Int32:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int>(xValues, ((PrimitiveArray<int>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int16:
                                    {
                                        BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, int, short, ToInt32Converter>(xValues, ((PrimitiveArray<short>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt16:
                                    {
                                        BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, int, ushort, ToInt32Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int8:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, sbyte, ToInt32Converter>(xValues, ((PrimitiveArray<sbyte>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, byte, ToInt32Converter>(xValues, ((PrimitiveArray<byte>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
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
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, double, uint, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, uint, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, uint, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, long, uint, ToInt64Converter>(xValues, ((PrimitiveArray<long>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.UInt64)
                        {
                            var result = GC.AllocateUninitializedArray<ulong>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, ulong, uint, ToUInt64Converter>(xValues, ((PrimitiveArray<ulong>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, long, uint, int, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<int>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, uint, short, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<short>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, uint, sbyte, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<sbyte>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<uint>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.UInt32:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, uint>(xValues, ((PrimitiveArray<uint>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt16:
                                    {
                                        BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, uint, ushort, ToUInt32Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, uint, byte, ToUInt32Converter>(xValues, ((PrimitiveArray<byte>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
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
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, short, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, float, short, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, short, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, short, ToInt64Converter>(xValues, ((PrimitiveArray<long>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, int, short, ToInt32Converter>(xValues, ((PrimitiveArray<int>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, short, uint, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.UInt16)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, int, short, ushort, ToInt32Converter, ToInt32Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<short>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.Int16:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, short>(xValues, ((PrimitiveArray<short>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.Int8:
                                    {
                                        BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, short, sbyte, ToInt16Converter>(xValues, ((PrimitiveArray<sbyte>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, short, byte, ToInt16Converter>(xValues, ((PrimitiveArray<byte>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
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
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, ushort, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, float, ushort, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, ushort, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ushort, ToInt64Converter>(xValues, ((PrimitiveArray<long>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, int, ushort, ToInt32Converter>(xValues, ((PrimitiveArray<int>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, ushort, uint, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, int, ushort, short, ToInt32Converter, ToInt32Converter>(xValues, ((PrimitiveArray<short>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, ushort, byte, ToInt32Converter, ToInt32Converter>(xValues, ((PrimitiveArray<byte>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        else
                        {
                            var result = GC.AllocateUninitializedArray<ushort>(length);

                            switch (y.Data.DataType.TypeId)
                            {
                                case ArrowTypeId.UInt16:
                                    {
                                        BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, ushort>(xValues, ((PrimitiveArray<ushort>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                case ArrowTypeId.UInt8:
                                    {
                                        BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, ushort, byte, ToUInt16Converter>(xValues, ((PrimitiveArray<byte>)y).Values, validityBitmap, nullCount, result);
                                        break;
                                    }
                                default: throw new NotSupportedException();
                            }

                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
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
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, sbyte, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, sbyte, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, sbyte, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, sbyte, ToInt64Converter>(xValues, ((PrimitiveArray<long>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, sbyte, ToInt32Converter>(xValues, ((PrimitiveArray<int>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, sbyte, uint, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<short>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, short, sbyte, ToInt16Converter>(xValues, ((PrimitiveArray<short>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.UInt16)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, sbyte, ushort, ToInt32Converter, ToInt32Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.UInt8)
                        {
                            var result = GC.AllocateUninitializedArray<short>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, short, sbyte, byte, ToInt16Converter, ToInt16Converter>(xValues, ((PrimitiveArray<byte>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<sbyte>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, sbyte>(xValues, ((PrimitiveArray<sbyte>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
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
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, double, byte, ToDoubleConverter>(xValues, ((PrimitiveArray<double>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Float)
                        {
                            var result = GC.AllocateUninitializedArray<float>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, float, byte, ToFloatConverter>(xValues, ((PrimitiveArray<float>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.HalfFloat)
                        {
                            var result = GC.AllocateUninitializedArray<Half>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, Half, byte, ToHalfFloatConverter>(xValues, ((PrimitiveArray<Half>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int64)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, byte, ToInt64Converter>(xValues, ((PrimitiveArray<long>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int32)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, byte, ToInt32Converter>(xValues, ((PrimitiveArray<int>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.UInt32)
                        {
                            var result = GC.AllocateUninitializedArray<long>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, long, byte, uint, ToInt64Converter, ToInt64Converter>(xValues, ((PrimitiveArray<uint>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int16)
                        {
                            var result = GC.AllocateUninitializedArray<short>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, short, byte, ToInt16Converter>(xValues, ((PrimitiveArray<short>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.UInt16)
                        {
                            var result = GC.AllocateUninitializedArray<int>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, int, byte, ushort, ToInt32Converter, ToInt32Converter>(xValues, ((PrimitiveArray<ushort>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.Int8)
                        {
                            var result = GC.AllocateUninitializedArray<short>(length);
                            BinaryOperatorExecutor.InvokeOperatorWithWidening<TBinaryOperator, short, byte, sbyte, ToInt16Converter, ToInt16Converter>(xValues, ((PrimitiveArray<sbyte>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        if (y.Data.DataType.TypeId == ArrowTypeId.UInt8)
                        {
                            var result = GC.AllocateUninitializedArray<byte>(length);
                            BinaryOperatorExecutor.InvokeOperator<TBinaryOperator, byte>(xValues, ((PrimitiveArray<byte>)y).Values, validityBitmap, nullCount, result);
                            return ArrowArrayFactory.BuildArray(result, validityBitmap, nullCount);
                        }
                        throw new NotSupportedException();
                    }

                    #endregion

                    throw new NotSupportedException();
            }

            throw new NotSupportedException();
        }
        #endregion
    }
}
