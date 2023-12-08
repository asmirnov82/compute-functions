using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.Converters
{
    internal readonly struct ToUInt64Converter :
        IConverter<ulong, uint>,
        IConverter<ulong, ushort>,
        IConverter<ulong, byte>,
        IWidener<ulong, uint>
    {
        #region IConverter<ulong, uint>
        static bool IConverter<ulong, uint>.SupportVectorization => false;

        public static ulong Convert(uint value)
        {
            return value;
        }

        public static Vector128<ulong> Convert(Vector128<uint> vector) => throw new NotSupportedException();
        public static Vector256<ulong> Convert(Vector256<uint> vector) => throw new NotSupportedException();
#if NET8_0_OR_GREATER
        public static Vector512<ulong> Convert(Vector512<uint> vector) => throw new NotSupportedException();
#endif
        #endregion

        #region IConverter<ulong, ushort>
        static bool IConverter<ulong, ushort>.SupportVectorization => false;

        public static ulong Convert(ushort value)
        {
            return value;
        }

        public static Vector128<ulong> Convert(Vector128<ushort> vector) => throw new NotSupportedException();
        public static Vector256<ulong> Convert(Vector256<ushort> vector) => throw new NotSupportedException();
#if NET8_0_OR_GREATER
        public static Vector512<ulong> Convert(Vector512<ushort> vector) => throw new NotSupportedException();
#endif
        #endregion

        #region IConverter<ulong, byte>
        static bool IConverter<ulong, byte>.SupportVectorization => false;

        public static ulong Convert(byte value)
        {
            return value;
        }

        public static Vector128<ulong> Convert(Vector128<byte> vector) => throw new NotSupportedException();
        public static Vector256<ulong> Convert(Vector256<byte> vector) => throw new NotSupportedException();
#if NET8_0_OR_GREATER
        public static Vector512<ulong> Convert(Vector512<byte> vector) => throw new NotSupportedException();
#endif
        #endregion

        #region IWidener<ulong, uint>
        public static ulong Widen(uint value)
        {
            return value;
        }

        public static (Vector128<ulong> Lower, Vector128<ulong> Upper) Widen(Vector128<uint> vector)
        {
            return Vector128.Widen(vector);
        }

        public static (Vector256<ulong> Lower, Vector256<ulong> Upper) Widen(Vector256<uint> vector)
        {
            return Vector256.Widen(vector);
        }

#if NET8_0_OR_GREATER
        public static (Vector512<ulong> Lower, Vector512<ulong> Upper) Widen(Vector512<uint> vector)
        {
            return Vector512.Widen(vector);
        }
#endif
        #endregion
    }
}
