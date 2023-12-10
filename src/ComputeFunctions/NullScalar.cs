using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Types;

namespace Gimpo.ComputeFunctions
{
    /// <summary>
    /// Represents a single Null value.
    /// </summary>
    public sealed class NullScalar : Scalar
    {
        public static readonly NullScalar Instance = new();

        private NullScalar() : base(NullType.Default)
        {}

        public override bool IsNumeric => false;

        protected override object? GetValue() => null;
    }
}
