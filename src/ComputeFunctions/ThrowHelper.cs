using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions
{
    internal static class ThrowHelper
    {
        [DoesNotReturn]
        public static void ThrowArgument_FunctionIncorrectAmountOfArguments(string paramName) =>
            throw new ArgumentException(Resources.FunctionIncorrectAmountOfArguments, paramName);

        [DoesNotReturn]
        public static void ThrowArgument_FunctionArgumentsArrayLengthMismatch() =>
            throw new ArgumentException(Resources.FunctionArgumentsArrayLengthMismatch);

        [DoesNotReturn]
        public static void Throw_DatumIncorrectVariantAccess() =>
            throw new InvalidOperationException(Resources.DatumIncorrectVariantAccess);

        public static void Throw_FunctionWithNameAlreadyExistsInTheRegistry(string functionName, string paramName) =>
            throw new ArgumentException(String.Format(Resources.FunctionWithNameAlreadyExistsInTheRegistry, functionName), paramName);
    }
}
