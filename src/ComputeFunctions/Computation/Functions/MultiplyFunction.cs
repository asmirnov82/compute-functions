using Gimpo.ComputeFunctions.Computation.Operators;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.Functions
{
    internal class MultiplyFunction : BinaryFunction<MultiplyOperator>
    {
        public MultiplyFunction() : base("multiply")
        {
        }
    }
}
