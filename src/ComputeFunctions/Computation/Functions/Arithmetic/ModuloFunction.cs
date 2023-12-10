using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Gimpo.ComputeFunctions.Computation.Operators;

namespace Gimpo.ComputeFunctions.Computation.Functions.Arithmetic
{
    internal class ModuloFunction : BinaryFunction<ModuloOperator>
    {
        private static readonly IFunction _function = new ModuloFunction();

        public static IFunction Instance => _function;

        private ModuloFunction() : base("modulo")
        {
        }
    }
}
