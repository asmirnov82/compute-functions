using Apache.Arrow;

namespace Gimpo.ComputeFunctions.Computation
{
    /// <summary>
    /// IFunction interface represents compute operations over inputs of possibly varying types.
    /// </summary>
    public interface IFunction
    {
        /// <summary>
        /// The name of the function.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Contains the number of required arguments for the function (or the minimum number for functions with variable number of arguments).
        /// </summary>
        int ArgumentCount { get; }

        /// <summary>
        /// True for functions supporting variable number of agruments.
        /// </summary>
        bool IsVariableArgumentCount { get; }

        FunctionKind Kind { get; }

        /// <summary>
        /// Execute function.
        /// </summary>
        /// <param name="args">Collection of input arguments.</param>
        /// <returns></returns>
        Datum Execute(IReadOnlyList<Datum> args);
    }
}
