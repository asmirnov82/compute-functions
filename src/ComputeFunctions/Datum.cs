using Apache.Arrow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions
{
    /// <summary>
    /// Variant type for various Gimpo Compute Function data structures (input and output).
    /// </summary>
    public struct Datum
    {
        private readonly object? _data;

        public readonly DatumKind Kind;

        public Datum()
        {
            Kind = DatumKind.None;
        }

        public Datum(IArrowArray arrowArray)
        {
            Kind = DatumKind.Array;
            _data = arrowArray;
        }

        public Datum(Table table)
        {
            Kind = DatumKind.Table;
            _data = table;
        }

        public Datum(Scalar scalar)
        {
            Kind = DatumKind.Scalar;
            _data = scalar;
        }

        /// <summary>
        /// Retrieve the stored array as Array.
        /// </summary>
        public IArrowArray Array
        {
            get
            {
                if (Kind != DatumKind.Array)
                    ThrowHelper.Throw_DatumIncorrectVariantAccess();

                return (IArrowArray)_data;
            }
        }

        /// <summary>
        /// Retrieve the stored scalar as Scalar.
        /// </summary>
        public Scalar Scalar
        {
            get
            {
                if (Kind != DatumKind.Scalar)
                    ThrowHelper.Throw_DatumIncorrectVariantAccess();

                return (Scalar)_data;
            }
        }

    }
}
