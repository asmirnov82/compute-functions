using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Types;
using Apache.Arrow;
using Gimpo.ComputeFunctions.Computation.Executors;
using Gimpo.ComputeFunctions.Computation.Converters;

namespace Gimpo.ComputeFunctions.Computation
{
    /// <summary>
    /// Aggregation function reduces the input to a single output value.
    /// Output is Int64, UInt64 or Double, depending on the input type.
    /// </summary>
    /// <typeparam name="TAggregationOperator">Aggregation operator.</typeparam>
    internal class AggregationWidenTypeFunction<TAggregationOperator> : IFunction
        where TAggregationOperator : struct, IAggregationOperator
    {
        private readonly string _name;
        public string Name => _name;

        public int ArgumentCount => 1;
        public bool IsVariableArgumentCount => false;

        public FunctionKind Kind => FunctionKind.ScalarAggregate;

        public AggregationWidenTypeFunction(string name)
        {
            _name = name;
        }

        public Datum Execute(IReadOnlyList<Datum> args)
        {
            if (args.Count != 1)
                ThrowHelper.ThrowArgument_FunctionIncorrectAmountOfArguments(nameof(args));

            var arg = args[0];

            if (arg.Kind == DatumKind.Array)
            {
                return new Datum(Execute(arg.Array));
            }

            throw new NotImplementedException(); //TODO
        }

        public static Scalar Execute(IArrowArray arg)
        {
            if (arg.Data.DataType.TypeId == ArrowTypeId.Null)
                return NullScalar.Instance;

            var x = (Apache.Arrow.Array)arg;

            switch (x.Data.DataType.TypeId)
            {
                case ArrowTypeId.Double:
                    return Scalar.Create(InvokeOperator<double>(x));
                case ArrowTypeId.Float:
                    return Scalar.Create(InvokeOperatorWithWidening<double, float, ToDoubleConverter>(x));
                case ArrowTypeId.HalfFloat:
                    return Scalar.Create(InvokeOperator<double, Half, ToDoubleConverter>(x));
                case ArrowTypeId.Int64:
                    return Scalar.Create(InvokeOperator<long>(x));
                case ArrowTypeId.UInt64:
                    return Scalar.Create(InvokeOperator<ulong>(x));
                case ArrowTypeId.Int32:
                    return Scalar.Create(InvokeOperatorWithWidening<long, int, ToInt64Converter>(x));
                case ArrowTypeId.UInt32:
                    return Scalar.Create(InvokeOperatorWithWidening<ulong, uint, ToUInt64Converter>(x));
                case ArrowTypeId.Int16:
                    return Scalar.Create(InvokeOperatorWithWidening<long, int, short, ToInt64Converter, ToInt32Converter>(x));
                case ArrowTypeId.UInt16:
                    return Scalar.Create(InvokeOperatorWithWidening<ulong, uint, ushort, ToUInt64Converter, ToUInt32Converter>(x));
                case ArrowTypeId.Int8:
                    return Scalar.Create(InvokeOperatorWithWidening<long, short, sbyte, ToInt64Converter, ToInt16Converter>(x));
                case ArrowTypeId.UInt8:
                    return Scalar.Create(InvokeOperatorWithWidening<ulong, ushort, byte, ToUInt64Converter, ToUInt16Converter>(x));
            }

            throw new NotSupportedException();
        }

        private static TResult InvokeOperator<TResult>(Apache.Arrow.Array array)
            where TResult : unmanaged, INumber<TResult>, IMinMaxValue<TResult>
        {
            return AggregationOperatorExecutor.InvokeOperator<TAggregationOperator, TResult>(((PrimitiveArray<TResult>)array).Values, array.NullBitmapBuffer.Span, array.NullCount);
        }

        private static TResult InvokeOperator<TResult, T, TConverter>(Apache.Arrow.Array array)
            where TResult : unmanaged, INumber<TResult>, IMinMaxValue<TResult>
            where T : unmanaged, INumber<T>
            where TConverter : unmanaged, IConverter<TResult, T>
        {
            return AggregationOperatorExecutor.InvokeOperator<TAggregationOperator, TResult, T, TConverter>(((PrimitiveArray<T>)array).Values, array.NullBitmapBuffer.Span, array.NullCount);
        }

        private static TResult InvokeOperatorWithWidening<TResult, T, TWidener>(Apache.Arrow.Array array)
            where TResult : unmanaged, INumber<TResult>, IMinMaxValue<TResult>
            where T : unmanaged, INumber<T>
            where TWidener : unmanaged, IWidener<TResult, T>
        {
            return AggregationOperatorExecutor.InvokeOperatorWithWidening<TAggregationOperator, TResult, T, TWidener>(((PrimitiveArray<T>)array).Values, array.NullBitmapBuffer.Span, array.NullCount);
        }

        private static TResult InvokeOperatorWithWidening<TResult, TWidened, T, TConverter, TWidener>(Apache.Arrow.Array array)
            where TResult : unmanaged, INumber<TResult>, IMinMaxValue<TResult>
             where TWidened : unmanaged, INumber<TWidened>
            where T : unmanaged, INumber<T>
            where TConverter : unmanaged, IConverter<TResult, TWidened>
            where TWidener : unmanaged, IWidener<TWidened, T>
        {
           return AggregationOperatorExecutor.InvokeOperator<TAggregationOperator, TResult, TWidened, T, TConverter, TWidener>(((PrimitiveArray<T>)array).Values, array.NullBitmapBuffer.Span, array.NullCount);
        }
    }
}
