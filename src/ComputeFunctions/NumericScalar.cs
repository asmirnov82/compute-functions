using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Types;

namespace Gimpo.ComputeFunctions
{
    /// <summary>
    /// Represents a single numeric value with a specific DataType.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class NumericScalar<T> : Scalar
        where T : unmanaged, INumber<T>
    {
        private readonly T _value;

        public new T Value => _value;

        public override bool IsNumeric => true;

        public NumericScalar(T value, IArrowType valueType) : base(valueType)
        {
            _value = value;
        }

        protected override object GetValue() => _value;
    }
}
