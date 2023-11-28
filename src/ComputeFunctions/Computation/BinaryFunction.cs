using Apache.Arrow;
using Apache.Arrow.Types;
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

            return new Datum(Execute(args[0].Array, args[1].Array));
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
                     
            return BinaryOperatorExecutor.Execute<TBinaryOperator>(x, y, resValidityBitmap);
        }

        
    }
}
