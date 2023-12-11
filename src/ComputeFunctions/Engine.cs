using Apache.Arrow;
using Gimpo.ComputeFunctions.Computation;
using Gimpo.ComputeFunctions.Computation.Functions.Aggregation;
using Gimpo.ComputeFunctions.Computation.Functions.Arithmetic;
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
            //Register Arithmetic functions
            RegisterFunction(AddFunction.Instance);
            RegisterFunction(SubtractFunction.Instance);
            RegisterFunction(MultiplyFunction.Instance);
            RegisterFunction(DivideFunction.Instance);
            RegisterFunction(ModuloFunction.Instance);

            //Register Aggregation functions
            RegisterFunction(SumFunction.Instance);
            RegisterFunction(MaxFunction.Instance);
            RegisterFunction(MinFunction.Instance);
        }

        /// <summary>
        /// Execute registered function by it's name.
        /// </summary>
        /// <param name="functionName">Name of the function.</param>
        /// <param name="args">List of function arguments.</param>
        /// <returns></returns>
        public static Datum CallFunction(string functionName, IReadOnlyList<Datum> args)
        {
            var function = GetFunctionByName(functionName);
            return function.Execute(args);
        }

        /// <summary>
        /// Fetch funtion from the registry by it's name.
        /// </summary>
        /// <param name="functionName">Name of the function.</param>
        /// <returns>Instance of the function.</returns>
        public static IFunction GetFunctionByName(string functionName)
        {
            return _functions[functionName];
        }

        /// <summary>
        /// Checks the ability to register function with provided name.
        /// </summary>
        /// <param name="functionName">Name of the function.</param>
        /// <returns>Returns true if function name can be added to the registry.</returns>
        public static bool CanRegisterFunctionName(string functionName)
        {
            return !_functions.ContainsKey(functionName);
        }

        /// <summary>
        /// Adds function to the function register.
        /// </summary>
        /// <param name="function"></param>
        public static void RegisterFunction(IFunction function)
        {
            if (!_functions.TryAdd(function.Name, function))
                ThrowHelper.Throw_FunctionWithNameAlreadyExistsInTheRegistry(function.Name, nameof(function));
        }

        //Arithmetic
        public static IArrowArray Add(IArrowArray arg1, Scalar arg2) => AddFunction.Execute(arg1, arg2);
        public static IArrowArray Add(Scalar arg1, IArrowArray arg2) => AddFunction.Execute(arg1, arg2);
        public static IArrowArray Add (IArrowArray arg1, IArrowArray arg2) => AddFunction.Execute(arg1, arg2);
        
        public static IArrowArray Divide(IArrowArray arg1, IArrowArray arg2) => DivideFunction.Execute(arg1, arg2);
        public static IArrowArray Divide(Scalar arg1, IArrowArray arg2) => DivideFunction.Execute(arg1, arg2);
        public static IArrowArray Divide(IArrowArray arg1, Scalar arg2) => DivideFunction.Execute(arg1, arg2);

        public static IArrowArray Multiply(IArrowArray arg1, IArrowArray arg2) => MultiplyFunction.Execute(arg1, arg2);
        public static IArrowArray Multiply(Scalar arg1, IArrowArray arg2) => MultiplyFunction.Execute(arg1, arg2);
        public static IArrowArray Multiply(IArrowArray arg1, Scalar arg2) => MultiplyFunction.Execute(arg1, arg2);

        public static IArrowArray Subtract(IArrowArray arg1, IArrowArray arg2) => SubtractFunction.Execute(arg1, arg2);
        public static IArrowArray Subtract(Scalar arg1, IArrowArray arg2) => SubtractFunction.Execute(arg1, arg2);
        public static IArrowArray Subtract(IArrowArray arg1, Scalar arg2) => SubtractFunction.Execute(arg1, arg2);

        public static IArrowArray Modulo(IArrowArray arg1, IArrowArray arg2) => ModuloFunction.Execute(arg1, arg2);
        public static IArrowArray Modulo(Scalar arg1, IArrowArray arg2) => ModuloFunction.Execute(arg1, arg2);
        public static IArrowArray Modulo(IArrowArray arg1, Scalar arg2) => ModuloFunction.Execute(arg1, arg2);

        //Aggregations
        public static Scalar Max(IArrowArray arg) => MaxFunction.Execute(arg);
        public static Scalar Min(IArrowArray arg) => MinFunction.Execute(arg);
        public static Scalar Sum(IArrowArray arg) => SumFunction.Execute(arg);
    }
}
