using Apache.Arrow;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gimpo.ComputeFunctions.Tests
{
    public partial class EngineTests
    {
        [Theory]
        [InlineData(new long[] { 0 }, new uint[] { 1 })]
        [InlineData(new long[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, new uint[] { 1, 2, 3, 4, uint.MaxValue, 33, 44, 128, 256 })]
        public void Arithmetic_Long_UInt_WithoutNull_Test(long[] left, uint[] right)
        {
            //Arrange
            var arg1 = new Int64Array.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new UInt32Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            Int64Array array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);
        }
    }
}
