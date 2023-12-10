using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Types;

namespace Gimpo.ComputeFunctions
{

    /// <summary>
    /// Represents a single value with a specific DataType.
    /// Scalars are useful for passing single value inputs to compute functions,
    /// or for representing individual array elements. 
    /// </summary>
    public abstract class Scalar
    {
        #region Factory Methods
        public static Scalar Create(long value) => new NumericScalar<long>(value, Int64Type.Default);
        public static Scalar Create(ulong value) => new NumericScalar<ulong>(value, UInt64Type.Default);
        public static Scalar Create(int value) => new NumericScalar<int>(value, Int32Type.Default);
        public static Scalar Create(uint value) => new NumericScalar<uint>(value, UInt32Type.Default);
        public static Scalar Create(short value) => new NumericScalar<short>(value, Int16Type.Default);
        public static Scalar Create(ushort value) => new NumericScalar<ushort>(value, UInt16Type.Default);
        public static Scalar Create(sbyte value) => new NumericScalar<sbyte>(value, Int8Type.Default);
        public static Scalar Create(byte value) => new NumericScalar<byte>(value, UInt8Type.Default);

        public static Scalar Create(double value) => new NumericScalar<double>(value, DoubleType.Default);
        public static Scalar Create(float value) => new NumericScalar<float>(value, FloatType.Default);
        public static Scalar Create(Half value) => new NumericScalar<Half>(value, HalfFloatType.Default);
        #endregion

        public readonly IArrowType ValueType;

        public object? Value => GetValue();

        public abstract bool IsNumeric { get; }

        protected abstract object? GetValue();

        protected Scalar (IArrowType valueType)
        {
            ValueType = valueType;
        }
    }
}
