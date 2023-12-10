using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Gimpo.ComputeFunctions.Computation.Operators;

namespace Gimpo.ComputeFunctions.Computation.Functions.Aggregation
{
    internal class MinFunction : AggregationFunction<MinOperator>
    {
        private static readonly IFunction _function = new MinFunction();
        public static IFunction Instance => _function;

        private MinFunction() : base("min")
        {
        }
    }
}
