using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.Converters
{
    internal readonly struct ToInt64Converter :
        IConverter<long, ulong>,
        IConverter<long, int>,
        IConverter<long, uint>,
        IConverter<long, short>,
        IConverter<long, ushort>,
        IConverter<long, sbyte>,
        IConverter<long, byte>,
        IWidener<long, int>,
        IWidener<long, uint>
    {
        #region IConverter<long, ulong>
        static bool IConverter<long, ulong>.SupportVectorization => true;

        public static long Convert(ulong value)
        {
            return (long)value;
        }

        public static Vector128<long> Convert(Vector128<ulong> vector)
        {
            return Vector128.AsInt64(vector);
        }

        public static Vector256<long> Convert(Vector256<ulong> vector)
        {
            return Vector256.AsInt64(vector);
        }
#if NET8_0_OR_GREATER
        public static Vector512<long> Convert(Vector512<ulong> vector)
        {
            return Vector512.AsInt64(vector);
        }
#endif
        #endregion

        #region IConverter<long, int>
        static bool IConverter<long, int>.SupportVectorization => false;

        public static long Convert(int value)
        {
            return value;
        }

        public static Vector128<long> Convert(Vector128<int> vector) => throw new NotSupportedException();
        public static Vector256<long> Convert(Vector256<int> vector) => throw new NotSupportedException();
#if NET8_0_OR_GREATER
        public static Vector512<long> Convert(Vector512<int> vector) => throw new NotSupportedException();
#endif
        #endregion

        #region IConverter<long, uint>
        static bool IConverter<long, uint>.SupportVectorization => false;

        public static long Convert(uint value)
        {
            return value;
        }

        public static Vector128<long> Convert(Vector128<uint> vector) => throw new NotSupportedException();
        public static Vector256<long> Convert(Vector256<uint> vector) => throw new NotSupportedException();
#if NET8_0_OR_GREATER
        public static Vector512<long> Convert(Vector512<uint> vector) => throw new NotSupportedException();
#endif
        #endregion

        #region IConverter<long, short>
        static bool IConverter<long, short>.SupportVectorization => false;

        public static long Convert(short value)
        {
            return value;
        }

        public static Vector128<long> Convert(Vector128<short> vector) => throw new NotSupportedException();
        public static Vector256<long> Convert(Vector256<short> vector) => throw new NotSupportedException();
#if NET8_0_OR_GREATER
        public static Vector512<long> Convert(Vector512<short> vector) => throw new NotSupportedException();
#endif
        #endregion

        #region IConverter<long, ushort>
        static bool IConverter<long, ushort>.SupportVectorization => false;

        public static long Convert(ushort value)
        {
            return value;
        }

        public static Vector128<long> Convert(Vector128<ushort> vector) => throw new NotSupportedException();
        public static Vector256<long> Convert(Vector256<ushort> vector) => throw new NotSupportedException();
#if NET8_0_OR_GREATER
        public static Vector512<long> Convert(Vector512<ushort> vector) => throw new NotSupportedException();
#endif
        #endregion

        #region IConverter<long, sbyte>
        static bool IConverter<long, sbyte>.SupportVectorization => false;

        public static long Convert(sbyte value)
        {
            return value;
        }

        public static Vector128<long> Convert(Vector128<sbyte> vector) => throw new NotSupportedException();
        public static Vector256<long> Convert(Vector256<sbyte> vector) => throw new NotSupportedException();
#if NET8_0_OR_GREATER
        public static Vector512<long> Convert(Vector512<sbyte> vector)  => throw new NotSupportedException();
#endif
        #endregion

        #region IConverter<long, byte>
        static bool IConverter<long, byte>.SupportVectorization => false;

        public static long Convert(byte value)
        {
            return value;
        }

        public static Vector128<long> Convert(Vector128<byte> vector) => throw new NotSupportedException();
        public static Vector256<long> Convert(Vector256<byte> vector) => throw new NotSupportedException();
#if NET8_0_OR_GREATER
        public static Vector512<long> Convert(Vector512<byte> vector) => throw new NotSupportedException();
#endif
        #endregion

        #region IWidener<long, int>
        public static long Widen(int value)
        {
            return value;
        }

        public static (Vector128<long> Lower, Vector128<long> Upper) Widen(Vector128<int> vector)
        {
            return Vector128.Widen(vector);
        }

        public static (Vector256<long> Lower, Vector256<long> Upper) Widen(Vector256<int> vector)
        {
            return Vector256.Widen(vector);
        }

#if NET8_0_OR_GREATER
        public static (Vector512<long> Lower, Vector512<long> Upper) Widen(Vector512<int> vector)
        {
            return Vector512.Widen(vector);
        }
#endif
        #endregion

        #region IWidener<long, uint>
        public static long Widen(uint value)
        {
            return value;
        }

        public static (Vector128<long> Lower, Vector128<long> Upper) Widen(Vector128<uint> vector)
        {
            var (Lower, Upper) = Vector128.Widen(vector);
            return (Vector128.AsInt64(Lower), Vector128.AsInt64(Upper));
        }

        public static (Vector256<long> Lower, Vector256<long> Upper) Widen(Vector256<uint> vector)
        {
            var (Lower, Upper) = Vector256.Widen(vector);
            return (Vector256.AsInt64(Lower), Vector256.AsInt64(Upper));
        }

#if NET8_0_OR_GREATER
        public static (Vector512<long> Lower, Vector512<long> Upper) Widen(Vector512<uint> vector)
        {
            var (Lower, Upper) = Vector512.Widen(vector);
            return (Vector512.AsInt64(Lower), Vector512.AsInt64(Upper));
        }
#endif
        #endregion
    }
}
