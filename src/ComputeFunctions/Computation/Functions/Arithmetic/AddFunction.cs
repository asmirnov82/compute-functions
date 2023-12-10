using Gimpo.ComputeFunctions.Computation.Operators;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.Functions.Arithmetic
{
    internal sealed class AddFunction : BinaryFunction<AddOperator>
    {
        private static readonly IFunction _function = new AddFunction();

        public static IFunction Instance => _function;

        private AddFunction() : base("add")
        {
        }
    }
}
