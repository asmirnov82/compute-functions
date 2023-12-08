using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.Converters
{
    internal readonly struct ToInt32Converter :
        IConverter<int, sbyte>,
        IConverter<int, byte>,
        IConverter<int, short>,
        IConverter<int, ushort>,
        IWidener<int, short>,
        IWidener<int, ushort>
    {
        #region IConverter<int, sbyte>
        static bool IConverter<int, sbyte>.SupportVectorization => false;

        public static int Convert(sbyte value)
        {
            return value;
        }

        public static Vector128<int> Convert(Vector128<sbyte> vector) => throw new NotSupportedException();
        public static Vector256<int> Convert(Vector256<sbyte> vector) => throw new NotSupportedException();
#if NET8_0_OR_GREATER
        public static Vector512<int> Convert(Vector512<sbyte> vector)  => throw new NotSupportedException();
#endif
        #endregion

        #region IConverter<int, byte>
        static bool IConverter<int, byte>.SupportVectorization => false;

        public static int Convert(byte value)
        {
            return value;
        }

        public static Vector128<int> Convert(Vector128<byte> vector) => throw new NotSupportedException();
        public static Vector256<int> Convert(Vector256<byte> vector) => throw new NotSupportedException();
#if NET8_0_OR_GREATER
        public static Vector512<int> Convert(Vector512<byte> vector) => throw new NotSupportedException();
#endif
        #endregion

        #region IConverter<int, short>
        static bool IConverter<int, short>.SupportVectorization => false;

        public static int Convert(short value)
        {
            return value;
        }

        public static Vector128<int> Convert(Vector128<short> vector) => throw new NotSupportedException();
        public static Vector256<int> Convert(Vector256<short> vector) => throw new NotSupportedException();
#if NET8_0_OR_GREATER
        public static Vector512<int> Convert(Vector512<short> vector) => throw new NotSupportedException();
#endif
    #endregion

    #region IConverter<int, ushort>
    static bool IConverter<int, ushort>.SupportVectorization => false;

        public static int Convert(ushort value)
        {
            return value;
        }

        public static Vector128<int> Convert(Vector128<ushort> vector) => throw new NotSupportedException();
        public static Vector256<int> Convert(Vector256<ushort> vector) => throw new NotSupportedException();
#if NET8_0_OR_GREATER
        public static Vector512<int> Convert(Vector512<ushort> vector) => throw new NotSupportedException();
#endif
        #endregion

        #region IWidener<int, short>
        public static int Widen(short value)
        {
            return value;
        }

        public static (Vector128<int> Lower, Vector128<int> Upper) Widen(Vector128<short> vector)
        {
            return Vector128.Widen(vector); 
        }

        public static (Vector256<int> Lower, Vector256<int> Upper) Widen(Vector256<short> vector)
        {
            return Vector256.Widen(vector);
        }

#if NET8_0_OR_GREATER
        public static (Vector512<int> Lower, Vector512<int> Upper) Widen(Vector512<short> vector)
        {
            return Vector512.Widen(vector);
        }
#endif
        #endregion

        #region IWidener<int, ushort>
        public static int Widen(ushort value)
        {
            return value;
        }

        public static (Vector128<int> Lower, Vector128<int> Upper) Widen(Vector128<ushort> vector)
        {
            var (Lower, Upper) = Vector128.Widen(vector);
            return (Vector128.AsInt32(Lower), Vector128.AsInt32(Upper));
        }

        public static (Vector256<int> Lower, Vector256<int> Upper) Widen(Vector256<ushort> vector)
        {
            var (Lower, Upper) = Vector256.Widen(vector);
            return (Vector256.AsInt32(Lower), Vector256.AsInt32(Upper));
        }

#if NET8_0_OR_GREATER
        public static (Vector512<int> Lower, Vector512<int> Upper) Widen(Vector512<ushort> vector)
        {
            var (Lower, Upper) = Vector512.Widen(vector);
            return (Vector512.AsInt32(Lower), Vector512.AsInt32(Upper));
        }
#endif
        #endregion
    }
}
