using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Gimpo.ComputeFunctions.Computation.Operators;

namespace Gimpo.ComputeFunctions.Computation.Functions.Aggregation
{
    internal class MaxFunction : AggregationFunction<MaxOperator>
    {
        private static readonly IFunction _function = new MaxFunction();
        public static IFunction Instance => _function;

        private MaxFunction() : base("max")
        {
        }
    }
}
