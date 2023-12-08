using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Computation
{
    internal interface INumberVectorProvider<T>
        where T : unmanaged, INumber<T>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        Vector128<T> LoadVector128Unsafe(uint i);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        Vector256<T> LoadVector256Unsafe(uint i);

#if NET8_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        Vector512<T> LoadVector512Unsafe(uint i);
#endif
    }
}
