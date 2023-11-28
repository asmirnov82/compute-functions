using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation.Converters
{
    internal readonly struct ToDoubleConverter :
        IConverter<double, long>,
        IConverter<double, ulong>,
        IConverter<double, Half>,
        IConverter<double, short>,
        IConverter<double, ushort>,
        IConverter<double, sbyte>,
        IConverter<double, byte>,
        IWidener<double, float>,
        IWidener<double, int>,
        IWidener<double, uint>
    {
        #region IConverter<double, Half>
        static bool IConverter<double, Half>.CanVectorize => false;

        public static double Convert(Half value)
        {
            return (double)value;
        }

        public static Vector128<double> Convert(Vector128<Half> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<double> Convert(Vector256<Half> vector)
        {
            throw new NotSupportedException();
        }
#if NET8_0_OR_GREATER
        public static Vector512<double> Convert(Vector512<Half> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<double, long>
        static bool IConverter<double, long>.CanVectorize => true;

        public static double Convert(long value)
        {
            return value;
        }

        public static Vector128<double> Convert(Vector128<long> vector)
        {
            return Vector128.ConvertToDouble(vector);
        }

        public static Vector256<double> Convert(Vector256<long> vector)
        {
            return Vector256.ConvertToDouble(vector);
        }
#if NET8_0_OR_GREATER
        public static Vector512<double> Convert(Vector512<long> vector)
        {
            return Vector512.ConvertToDouble(vector);
        }
#endif
        #endregion

        #region IConverter<double, ulong>
        static bool IConverter<double, ulong>.CanVectorize => true;

        public static double Convert(ulong value)
        {
            return value;
        }

        public static Vector128<double> Convert(Vector128<ulong> vector)
        {
            return Vector128.ConvertToDouble(vector);
        }

        public static Vector256<double> Convert(Vector256<ulong> vector)
        {
            return Vector256.ConvertToDouble(vector);
        }
#if NET8_0_OR_GREATER
        public static Vector512<double> Convert(Vector512<ulong> vector)
        {
            return Vector512.ConvertToDouble(vector);
        }
#endif
        #endregion

        #region IConverter<double, short>
        static bool IConverter<double, short>.CanVectorize => false;

        public static double Convert(short value)
        {
            return value;
        }

        public static Vector128<double> Convert(Vector128<short> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<double> Convert(Vector256<short> vector)
        {
            throw new NotSupportedException();
        }
#if NET8_0_OR_GREATER
        public static Vector512<double> Convert(Vector512<short> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<double, ushort>
        static bool IConverter<double, ushort>.CanVectorize => false;

        public static double Convert(ushort value)
        {
            return value;
        }

        public static Vector128<double> Convert(Vector128<ushort> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<double> Convert(Vector256<ushort> vector)
        {
            throw new NotSupportedException();
        }
#if NET8_0_OR_GREATER
        public static Vector512<double> Convert(Vector512<ushort> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<double, sbyte>
        static bool IConverter<double, sbyte>.CanVectorize => false;

        public static double Convert(sbyte value)
        {
            return value;
        }

        public static Vector128<double> Convert(Vector128<sbyte> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<double> Convert(Vector256<sbyte> vector)
        {
            throw new NotSupportedException();
        }
#if NET8_0_OR_GREATER
        public static Vector512<double> Convert(Vector512<sbyte> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IConverter<double, byte>
        static bool IConverter<double, byte>.CanVectorize => false;

        public static double Convert(byte value)
        {
            return value;
        }

        public static Vector128<double> Convert(Vector128<byte> vector)
        {
            throw new NotSupportedException();
        }

        public static Vector256<double> Convert(Vector256<byte> vector)
        {
            throw new NotSupportedException();
        }
#if NET8_0_OR_GREATER
        public static Vector512<double> Convert(Vector512<byte> vector)
        {
            throw new NotSupportedException();
        }
#endif
        #endregion

        #region IWidener<double, float>
        public static double Widen(float value)
        {
            return value;
        }

        public static (Vector128<double> Lower, Vector128<double> Upper) Widen(Vector128<float> vector)
        {
            return Vector128.Widen(vector);
        }

        public static (Vector256<double> Lower, Vector256<double> Upper) Widen(Vector256<float> vector)
        {
            return Vector256.Widen(vector);
        }

#if NET8_0_OR_GREATER
        public static (Vector512<double> Lower, Vector512<double> Upper) Widen(Vector512<float> vector)
        {
            return Vector512.Widen(vector);
        }
#endif
        #endregion

        #region IWidener<double, int>
        public static double Widen(int value)
        {
            return value;
        }

        public static (Vector128<double> Lower, Vector128<double> Upper) Widen(Vector128<int> vector)
        {
            var (Lower, Upper) = Vector128.Widen(vector);
            return (Vector128.ConvertToDouble(Lower), Vector128.ConvertToDouble(Upper));
        }

        public static (Vector256<double> Lower, Vector256<double> Upper) Widen(Vector256<int> vector)
        {
            var (Lower, Upper) = Vector256.Widen(vector);
            return (Vector256.ConvertToDouble(Lower), Vector256.ConvertToDouble(Upper));
        }

#if NET8_0_OR_GREATER
        public static (Vector512<double> Lower, Vector512<double> Upper) Widen(Vector512<int> vector)
        {
            var (Lower, Upper) = Vector512.Widen(vector);
            return (Vector512.ConvertToDouble(Lower), Vector512.ConvertToDouble(Upper));
        }
#endif
        #endregion

        #region IWidener<double, uint>
        public static double Widen(uint value)
        {
            return value;
        }

        public static (Vector128<double> Lower, Vector128<double> Upper) Widen(Vector128<uint> vector)
        {
            var (Lower, Upper) = Vector128.Widen(vector);
            return (Vector128.ConvertToDouble(Lower), Vector128.ConvertToDouble(Upper));
        }

        public static (Vector256<double> Lower, Vector256<double> Upper) Widen(Vector256<uint> vector)
        {
            var (Lower, Upper) = Vector256.Widen(vector);
            return (Vector256.ConvertToDouble(Lower), Vector256.ConvertToDouble(Upper));
        }

#if NET8_0_OR_GREATER
        public static (Vector512<double> Lower, Vector512<double> Upper) Widen(Vector512<uint> vector)
        {
            var (Lower, Upper) = Vector512.Widen(vector);
            return (Vector512.ConvertToDouble(Lower), Vector512.ConvertToDouble(Upper));
        }
#endif
        #endregion
    }
}
