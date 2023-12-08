using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.Converters
{
    internal readonly struct ToUInt16Converter :
        IConverter<ushort, byte>,
        IWidener<ushort, byte>
    {
        #region IConverter<ushort, byte>
        public static bool SupportVectorization => throw new NotImplementedException();

        public static ushort Convert(byte value) => throw new NotImplementedException();
        public static Vector128<ushort> Convert(Vector128<byte> vector) => throw new NotImplementedException();
        public static Vector256<ushort> Convert(Vector256<byte> vector) => throw new NotImplementedException();
#if NET8_0_OR_GREATER
        public static Vector512<ushort> Convert(Vector512<byte> vector) => throw new NotImplementedException();
#endif
        #endregion

        #region IWidener<ushort, byte>
        public static ushort Widen(byte value)
        {
            return value;
        }

        public static (Vector128<ushort> Lower, Vector128<ushort> Upper) Widen(Vector128<byte> vector)
        {
            return Vector128.Widen(vector);
        }

        public static (Vector256<ushort> Lower, Vector256<ushort> Upper) Widen(Vector256<byte> vector)
        {
            return Vector256.Widen(vector);
        }

#if NET8_0_OR_GREATER
        public static (Vector512<ushort> Lower, Vector512<ushort> Upper) Widen(Vector512<byte> vector)
        {
            return Vector512.Widen(vector);
        }
#endif
#endregion
    }
}
