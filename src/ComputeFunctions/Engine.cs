﻿using Apache.Arrow;
using Gimpo.ComputeFunctions.Computation;
using Gimpo.ComputeFunctions.Computation.Functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions
{
    /// <summary>
    /// 
    /// </summary>
    public static class Engine
    {
        private static readonly Dictionary<string, IFunction> _functions = new Dictionary<string, IFunction>(8);

        static Engine()
        {
            //Register functions
            RegisterFunction(new AddFunction());
            RegisterFunction(new SubtractFunction());
            RegisterFunction(new MultiplyFunction());
            RegisterFunction(new DivideFunction());
            RegisterFunction(new ModuloFunction());
        }
               
        public static Datum CallFunction(string functionName, IReadOnlyList<Datum> args)
        {
            var function = GetFunctionByName(functionName);
            return function.Execute(args);
        }

        public static IFunction GetFunctionByName(string functionName)
        {
            return _functions[functionName];
        }

        public static bool CanAddFunctionName(string functionName)
        {
            return !_functions.ContainsKey(functionName);
        }

        public static void RegisterFunction(IFunction function)
        {
            if (!_functions.TryAdd(function.Name, function))
                ThrowHelper.Throw_FunctionWithNameAlreadyExistsInTheRegistry(function.Name, nameof(function));
        }

        public static IArrowArray Add (IArrowArray arg1, IArrowArray arg2)
        {
            return AddFunction.Execute(arg1, arg2);
        }

        public static IArrowArray Divide(IArrowArray arg1, IArrowArray arg2)
        {
            return DivideFunction.Execute(arg1, arg2);
        }

        public static IArrowArray Multiply(IArrowArray arg1, IArrowArray arg2)
        {
            return MultiplyFunction.Execute(arg1, arg2);
        }

        public static IArrowArray Subtract(IArrowArray arg1, IArrowArray arg2)
        {
            return SubtractFunction.Execute(arg1, arg2);
        }

        public static IArrowArray Modulo(IArrowArray arg1, IArrowArray arg2)
        {
            return ModuloFunction.Execute(arg1, arg2);
        }
    }
}
