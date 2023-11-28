using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.Converters
{
    internal readonly struct ToUInt32Converter :
        IConverter<uint, byte>,
        IWidener<uint, ushort>
    {
        #region  IConverter<uint, byte>
        public static bool CanVectorize => false;

        public static uint Convert(byte value)
        {
            return value;
        }

        public static Vector128<uint> Convert(Vector128<byte> vector)
        {
            throw new NotImplementedException();
        }

        public static Vector256<uint> Convert(Vector256<byte> vector)
        {
            throw new NotImplementedException();
        }
#if NET8_0_OR_GREATER
        public static Vector512<uint> Convert(Vector512<byte> vector)
        {
            throw new NotImplementedException();
        }
#endif
        #endregion

        #region IWidener<uint, ushort>
        public static uint Widen(ushort value)
        {
            return value;
        }

        public static (Vector128<uint> Lower, Vector128<uint> Upper) Widen(Vector128<ushort> vector)
        {
            return Vector128.Widen(vector);
        }

        public static (Vector256<uint> Lower, Vector256<uint> Upper) Widen(Vector256<ushort> vector)
        {
            return Vector256.Widen(vector);
        }

#if NET8_0_OR_GREATER
        public static (Vector512<uint> Lower, Vector512<uint> Upper) Widen(Vector512<ushort> vector)
        {
            return Vector512.Widen(vector);
        }
#endif
        #endregion
    }
}
