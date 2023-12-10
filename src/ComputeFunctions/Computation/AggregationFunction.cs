using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using Gimpo.ComputeFunctions.Computation.Executors;

namespace Gimpo.ComputeFunctions.Computation
{    
    /// <summary>
    /// Aggregation function reduces the input to a single output value with output type the same as input type.
    /// </summary>
    /// <typeparam name="TAggregationOperator">Aggregation operator.</typeparam>
    internal abstract class AggregationFunction<TAggregationOperator> : IFunction
        where TAggregationOperator : struct, IAggregationOperator
    {
        private readonly string _name;
        public string Name => _name;

        public int ArgumentCount => 1;
        public bool IsVariableArgumentCount => false;

        public FunctionKind Kind => FunctionKind.ScalarAggregate;

        public AggregationFunction(string name)
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
                    return Scalar.Create(InvokeOperator<float>(x));
                case ArrowTypeId.HalfFloat:
                    return Scalar.Create(InvokeOperator<Half>(x));
                case ArrowTypeId.Int64:
                    return Scalar.Create(InvokeOperator<long>(x));
                case ArrowTypeId.UInt64:
                    return Scalar.Create(InvokeOperator<ulong>(x));
                case ArrowTypeId.Int32:
                    return Scalar.Create(InvokeOperator<int>(x));
                case ArrowTypeId.UInt32:
                    return Scalar.Create(InvokeOperator<uint>(x));
                case ArrowTypeId.Int16:
                    return Scalar.Create(InvokeOperator<short>(x));
                case ArrowTypeId.UInt16:
                    return Scalar.Create(InvokeOperator<ushort>(x));
                case ArrowTypeId.Int8:
                    return Scalar.Create(InvokeOperator<sbyte>(x));
                case ArrowTypeId.UInt8:
                    return Scalar.Create(InvokeOperator<byte>(x));
            }

            throw new NotSupportedException();
        }

        private static TResult InvokeOperator<TResult>(Apache.Arrow.Array array)
            where TResult : unmanaged, INumber<TResult>, IMinMaxValue<TResult>
        {
            return AggregationOperatorExecutor.InvokeOperator<TAggregationOperator,TResult>(((PrimitiveArray<TResult>)array).Values);
        }
    }
}
