using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.Converters
{
    internal readonly struct ToInt16Converter :
        IWidener<short, sbyte>,
        IWidener<short, byte>
    {
        #region IWidener<short, sbyte>
        public static short Widen(sbyte value)
        {
            return value;
        }

        public static (Vector128<short> Lower, Vector128<short> Upper) Widen(Vector128<sbyte> vector)
        {
            return Vector128.Widen(vector);
        }

        public static (Vector256<short> Lower, Vector256<short> Upper) Widen(Vector256<sbyte> vector)
        {
            return Vector256.Widen(vector);
        }

#if NET8_0_OR_GREATER
        public static (Vector512<short> Lower, Vector512<short> Upper) Widen(Vector512<sbyte> vector)
        {
            return Vector512.Widen(vector);
        }
#endif
        #endregion

        #region IWidener<short, byte>
        public static short Widen(byte value)
        {
            return value;
        }

        public static (Vector128<short> Lower, Vector128<short> Upper) Widen(Vector128<byte> vector)
        {
            var (Lower, Upper) = Vector128.Widen(vector);
            return (Vector128.AsInt16(Lower), Vector128.AsInt16(Upper));
        }

        public static (Vector256<short> Lower, Vector256<short> Upper) Widen(Vector256<byte> vector)
        {
            var (Lower, Upper) = Vector256.Widen(vector);
            return (Vector256.AsInt16(Lower), Vector256.AsInt16(Upper));
        }

#if NET8_0_OR_GREATER
        public static (Vector512<short> Lower, Vector512<short> Upper) Widen(Vector512<byte> vector)
        {
            var (Lower, Upper) = Vector512.Widen(vector);
            return (Vector512.AsInt16(Lower), Vector512.AsInt16(Upper));
        }
#endif
        #endregion
    }
}
