using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Gimpo.ComputeFunctions.Computation.Operators;

namespace Gimpo.ComputeFunctions.Computation.Functions
{
    internal class ModuloFunction : BinaryFunction<ModuloOperator>
    {
        public ModuloFunction() : base("modulo")
        {
        }
    }
}
