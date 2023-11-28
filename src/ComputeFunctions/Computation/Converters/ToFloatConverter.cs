using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.Converters
{
    internal readonly struct ToFloatConverter :
        IConverter<float, Half>,
        IConverter<float, long>,
        IConverter<float, ulong>,
        IConverter<float, int>,
        IConverter<float, uint>,
        IConverter<float, sbyte>,
        IConverter<float, byte>,
        IWidener<float, short>,
        IWidener<float, ushort>
    {
        #region IConverter<float, Half>
        static bool IConverter<float, Half>.CanVectorize => false;

        public static float Convert(Half value)
        {
            return (float)value;
        }

        public static Vector128<float> Convert(Vector128<Half> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<float> Convert(Vector256<Half> vector)
        {
            throw new NotSupportedException();
        }

#if NET8_0_OR_GREATER
        public static Vector512<float> Convert(Vector512<Half> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<float, long>
        static bool IConverter<float, long>.CanVectorize => false;

        public static float Convert(long value)
        {
            return value;
        }

        public static Vector128<float> Convert(Vector128<long> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<float> Convert(Vector256<long> vector)
        {
            throw new NotSupportedException();
        }

#if NET8_0_OR_GREATER
        public static Vector512<float> Convert(Vector512<long> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<float, ulong>
        static bool IConverter<float, ulong>.CanVectorize => false;

        public static float Convert(ulong value)
        {
            return value;
        }

        public static Vector128<float> Convert(Vector128<ulong> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<float> Convert(Vector256<ulong> vector)
        {
            throw new NotSupportedException();
        }

#if NET8_0_OR_GREATER
        public static Vector512<float> Convert(Vector512<ulong> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<float, int>
        static bool IConverter<float, int>.CanVectorize => true;

        public static float Convert(int value)
        {
            return value;
        }

        public static Vector128<float> Convert(Vector128<int> vector)
        {
            return Vector128.ConvertToSingle(vector);
        }

        public static Vector256<float> Convert(Vector256<int> vector)
        {
            return Vector256.ConvertToSingle(vector); 
        }

#if NET8_0_OR_GREATER
        public static Vector512<float> Convert(Vector512<int> vector)
        {
            return Vector512.ConvertToSingle(vector);
        }
#endif
        #endregion

        #region IConverter<float, uint>
        static bool IConverter<float, uint>.CanVectorize => true;

        public static float Convert(uint value)
        {
            return value;
        }

        public static Vector128<float> Convert(Vector128<uint> vector)
        {
            return Vector128.ConvertToSingle(vector);
        }

        public static Vector256<float> Convert(Vector256<uint> vector)
        {
            return Vector256.ConvertToSingle(vector);
        }

#if NET8_0_OR_GREATER
        public static Vector512<float> Convert(Vector512<uint> vector)
        {
            return Vector512.ConvertToSingle(vector);
        }
#endif
        #endregion

        #region IConverter<float, sbyte>
        static bool IConverter<float, sbyte>.CanVectorize => false;

        public static float Convert(sbyte value)
        {
            return value;
        }

        public static Vector128<float> Convert(Vector128<sbyte> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<float> Convert(Vector256<sbyte> vector)
        {
            throw new NotSupportedException();
        }

#if NET8_0_OR_GREATER
        public static Vector512<float> Convert(Vector512<sbyte> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<float, byte>
        static bool IConverter<float, byte>.CanVectorize => false;

        public static float Convert(byte value)
        {
            return value;
        }

        public static Vector128<float> Convert(Vector128<byte> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<float> Convert(Vector256<byte> vector)
        {
            throw new NotSupportedException();
        }

#if NET8_0_OR_GREATER
        public static Vector512<float> Convert(Vector512<byte> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IWidener<double, short>
        public static float Widen(short value)
        {
            return value;
        }

        public static (Vector128<float> Lower, Vector128<float> Upper) Widen(Vector128<short> vector)
        {
            var (Lower, Upper) = Vector128.Widen(vector);
            return (Vector128.ConvertToSingle(Lower), Vector128.ConvertToSingle(Upper));
        }

        public static (Vector256<float> Lower, Vector256<float> Upper) Widen(Vector256<short> vector)
        {
            var (Lower, Upper) = Vector256.Widen(vector);
            return (Vector256.ConvertToSingle(Lower), Vector256.ConvertToSingle(Upper));
        }

#if NET8_0_OR_GREATER
        public static (Vector512<float> Lower, Vector512<float> Upper) Widen(Vector512<short> vector)
        {
            var (Lower, Upper) = Vector512.Widen(vector);
            return (Vector512.ConvertToSingle(Lower), Vector512.ConvertToSingle(Upper));
        }
#endif
        #endregion

        #region IWidener<float, ushort>
        public static float Widen(ushort value)
        {
            return value;
        }

        public static (Vector128<float> Lower, Vector128<float> Upper) Widen(Vector128<ushort> vector)
        {
            var (Lower, Upper) = Vector128.Widen(vector);
            return (Vector128.ConvertToSingle(Lower), Vector128.ConvertToSingle(Upper));
        }

        public static (Vector256<float> Lower, Vector256<float> Upper) Widen(Vector256<ushort> vector)
        {
            var (Lower, Upper) = Vector256.Widen(vector);
            return (Vector256.ConvertToSingle(Lower), Vector256.ConvertToSingle(Upper));
        }

#if NET8_0_OR_GREATER
        public static (Vector512<float> Lower, Vector512<float> Upper) Widen(Vector512<ushort> vector)
        {
            var (Lower, Upper) = Vector512.Widen(vector);
            return (Vector512.ConvertToSingle(Lower), Vector512.ConvertToSingle(Upper));
        }
#endif
        #endregion
    }
}
