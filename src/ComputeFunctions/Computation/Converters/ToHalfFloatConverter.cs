using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.Converters
{
    internal readonly struct ToHalfFloatConverter :
        IConverter<Half, long>,
        IConverter<Half, ulong>,
        IConverter<Half, int>,
        IConverter<Half, uint>,
        IConverter<Half, sbyte>,
        IConverter<Half, byte>,
        IConverter<Half, short>,
        IConverter<Half, ushort>
    {
        #region IConverter<Half, long>
        static bool IConverter<Half, long>.CanVectorize => false;

        public static Half Convert(long value)
        {
            return (Half)value;
        }

        public static Vector128<Half> Convert(Vector128<long> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<Half> Convert(Vector256<long> vector)
        {
            throw new NotSupportedException();
        }

#if NET8_0_OR_GREATER
        public static Vector512<Half> Convert(Vector512<long> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<Half, ulong>
        static bool IConverter<Half, ulong>.CanVectorize => false;

        public static Half Convert(ulong value)
        {
            return (Half)value;
        }

        public static Vector128<Half> Convert(Vector128<ulong> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<Half> Convert(Vector256<ulong> vector)
        {
            throw new NotSupportedException();
        }

#if NET8_0_OR_GREATER
        public static Vector512<Half> Convert(Vector512<ulong> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<Half, int>
        static bool IConverter<Half, int>.CanVectorize => false;

        public static Half Convert(int value)
        {
            return (Half)value;
        }

        public static Vector128<Half> Convert(Vector128<int> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<Half> Convert(Vector256<int> vector)
        {
            throw new NotSupportedException();
        }

#if NET8_0_OR_GREATER
        public static Vector512<Half> Convert(Vector512<int> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<Half, uint>

        static bool IConverter<Half, uint>.CanVectorize => false;

        public static Half Convert(uint value)
        {
            return (Half)value;
        }

        public static Vector128<Half> Convert(Vector128<uint> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<Half> Convert(Vector256<uint> vector)
        {
            throw new NotSupportedException();
        }

#if NET8_0_OR_GREATER
        public static Vector512<Half> Convert(Vector512<uint> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion


        #region IConverter<Half, sbyte>
        static bool IConverter<Half, sbyte>.CanVectorize => false;

        public static Half Convert(sbyte value)
        {
            return (Half)value;
        }

        public static Vector128<Half> Convert(Vector128<sbyte> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<Half> Convert(Vector256<sbyte> vector)
        {
            throw new NotSupportedException();
        }

#if NET8_0_OR_GREATER
        public static Vector512<Half> Convert(Vector512<sbyte> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<Half, byte>
        static bool IConverter<Half, byte>.CanVectorize => false;

        public static Half Convert(byte value)
        {
            return (Half)value;
        }

        public static Vector128<Half> Convert(Vector128<byte> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<Half> Convert(Vector256<byte> vector)
        {
            throw new NotSupportedException();
        }

#if NET8_0_OR_GREATER
        public static Vector512<Half> Convert(Vector512<byte> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<Half, short>
        static bool IConverter<Half, short>.CanVectorize => false;

        public static Half Convert(short value)
        {
            return (Half)value;
        }

        public static Vector128<Half> Convert(Vector128<short> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<Half> Convert(Vector256<short> vector)
        {
            throw new NotSupportedException();
        }

#if NET8_0_OR_GREATER
        public static Vector512<Half> Convert(Vector512<short> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<Half, ushort>
        static bool IConverter<Half, ushort>.CanVectorize => false;

        public static Half Convert(ushort value)
        {
            return (Half)value;
        }

        public static Vector128<Half> Convert(Vector128<ushort> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<Half> Convert(Vector256<ushort> vector)
        {
            throw new NotSupportedException();
        }

#if NET8_0_OR_GREATER
        public static Vector512<Half> Convert(Vector512<ushort> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

    }
}
