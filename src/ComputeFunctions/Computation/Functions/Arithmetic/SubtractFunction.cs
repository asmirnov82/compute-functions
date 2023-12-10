using Gimpo.ComputeFunctions.Computation.Operators;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.Functions.Arithmetic
{
    internal class SubtractFunction : BinaryFunction<SubtractOperator>
    {
        private static readonly IFunction _function = new SubtractFunction();

        public static IFunction Instance => _function;

        private SubtractFunction() : base("subtract")
        {
        }
    }
}
