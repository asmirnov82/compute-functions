using Apache.Arrow;
using FluentAssertions;
using Gimpo.ComputeFunctions.Computation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Tests
{
    public partial class EngineTests
    {
        [Fact]
        public void Test_IndirectFunctionInvokeByName()
        {
            //Arrange
            var arg1 = new Datum(
                new Int32Array.Builder()
                    .AppendRange(new[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 })
                    .Build());

            var arg2 = new Datum(
                new Int32Array.Builder()
                    .AppendRange(new[] { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 })
                    .Build());

            //Act Addition
            Datum result = Engine.CallFunction("add", new Datum[] {arg1, arg2});

            //Assert
            result.Should().NotBeNull();

            result.Kind.Should().Be(DatumKind.Array);
            result.Array.Should().NotBeNull();

            result.Array.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Array.Length.Should().Be(11);
        }

        [Fact]
        public void Test_FunctionLookup()
        {
            IFunction function;

            string[] names = [ "add", "subtract", "multiply", "divide" ];

            foreach (var name in names)
            {
                //Act
                function = Engine.GetFunctionByName(name);

                //Assert
                function.Should().NotBeNull();
                function.Name.Should().Be(name);
                function.IsVariableArgumentCount.Should().BeFalse();
                function.ArgumentCount.Should().Be(2);
            }
        }
    }
}
