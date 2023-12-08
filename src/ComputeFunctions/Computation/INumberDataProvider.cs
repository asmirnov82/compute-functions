using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Gimpo.ComputeFunctions.Computation
{
    internal interface INumberDataProvider<T> : INumberVectorProvider<T>
        where T : unmanaged, INumber<T>
    {
        static abstract bool SupportVectorization { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        T GetValue(int i);
    }
}
