using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation
{
    public enum FunctionKind
    {
        /// A function that performs scalar data operations on whole arrays of
        /// data. Can generally process Array or Scalar values. The size of the
        /// output will be the same as the size (or broadcasted size, in the case
        /// of mixing Array and Scalar inputs) of the input.
        Scalar,

        /// A function with array input and output whose behavior depends on the
        /// values of the entire arrays passed, rather than the value of each scalar
        /// value.
        Vector,

        /// A function that computes scalar summary statistics from array input.
        ScalarAggregate
    }
}
