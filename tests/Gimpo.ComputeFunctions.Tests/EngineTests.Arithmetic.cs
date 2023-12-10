using Apache.Arrow;
using FluentAssertions;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Gimpo.ComputeFunctions.Tests
{
    public partial class EngineTests
    {
        #region Sum (as Widen Type Aggregation)
        [Fact]
        public void Test_ArithmeticsMultilplyWithNulls()
        {
            // Arrange
            var arg1 = new Int32Array.Builder().Append(4).Append(1).AppendNull().AppendNull().Build();
            var arg2 = new Int32Array.Builder().Append(2).AppendNull().Append(1).AppendNull().Build();

            // Act
            var result = Engine.Multiply(arg1, arg2);

            // Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.NullCount.Should().Be(3);
            result.Length.Should().Be(4);

            Int32Array array = (Int32Array)result;

            // Assert
            array.IsNull(0).Should().BeFalse();
            array.Values[0].Should().Be(8);  // 4 * 2

            array.IsNull(1).Should().BeTrue(); // 1 * null
            array.IsNull(2).Should().BeTrue(); // null * 1
            array.IsNull(3).Should().BeTrue(); // null * null
        }

        [Fact]
        public void Test_ArithmeticsDivideWithNulls()
        {
            // Arrange
            var arg1 = new Int32Array.Builder().Append(4).Append(1).AppendNull().AppendNull().Build();
            var arg2 = new Int32Array.Builder().Append(2).AppendNull().Append(1).AppendNull().Build();

            // Act
            var result = Engine.Divide (arg1, arg2);

            // Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.NullCount.Should().Be(3);
            result.Length.Should().Be(4);

            Int32Array array = (Int32Array)result;

            // Assert
            array.IsNull(0).Should().BeFalse();
            array.Values[0].Should().Be(2);  // 4 / 2

            array.IsNull(1).Should().BeTrue(); // 1 / null
            array.IsNull(2).Should().BeTrue(); // null / 1
            array.IsNull(3).Should().BeTrue(); // null / null
        }
        #endregion
    }
}
