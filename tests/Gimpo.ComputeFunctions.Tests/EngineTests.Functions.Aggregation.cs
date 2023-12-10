using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow;
using FluentAssertions;

namespace Gimpo.ComputeFunctions.Tests
{
    public partial class EngineTests
    {
        #region Sum
        [Fact]
        public void Test_AggregationSumOverInt32()
        {
            //Arrange
            var arg = new Datum(
                new Int32Array.Builder()
                    .AppendRange(new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 })
                    .Build());

            //Act Addition
            Datum result = Engine.CallFunction("sum", new Datum[] { arg });

            //Assert
            result.Should().NotBeNull();

            result.Kind.Should().Be(DatumKind.Scalar);
            result.Scalar.Should().NotBeNull();
            result.Scalar.IsNumeric.Should().BeTrue();
            result.Scalar.ValueType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);

            var sum = ((NumericScalar<long>)result.Scalar).Value;

            sum.Should().Be(120);
        }

        [Fact]
        public void Test_AggregationSumOverUInt16()
        {
            //Arrange
            var arg = new Datum(
                new UInt16Array.Builder()
                    .AppendRange(new ushort[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 })
                    .Build());

            //Act Addition
            Datum result = Engine.CallFunction("sum", new Datum[] { arg });

            //Assert
            result.Should().NotBeNull();

            result.Kind.Should().Be(DatumKind.Scalar);
            result.Scalar.Should().NotBeNull();
            result.Scalar.IsNumeric.Should().BeTrue();
            result.Scalar.ValueType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.UInt64);

            var sum = ((NumericScalar<ulong>)result.Scalar).Value;

            sum.Should().Be(120);
        }

        [Fact]
        public void Test_AggregationSumOverDouble()
        {
            //Arrange
            var arg = new Datum(
                new DoubleArray.Builder()
                    .AppendRange(new[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5 })
                    .Build());

            //Act Addition
            Datum result = Engine.CallFunction("sum", new Datum[] { arg });

            //Assert
            result.Should().NotBeNull();

            result.Kind.Should().Be(DatumKind.Scalar);
            result.Scalar.Should().NotBeNull();
            result.Scalar.IsNumeric.Should().BeTrue();
            result.Scalar.ValueType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Double);

            var sum = ((NumericScalar<double>)result.Scalar).Value;

            sum.Should().Be(12.0);
        }

        [Fact]
        public void Test_AggregationSumOverFloat()
        {
            //Arrange
            var values = new[] { 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f, 1.1f, 1.2f, 1.3f, 1.4f, 1.5f };
            var arg = new Datum(
                new FloatArray.Builder()
                    .AppendRange(values)
                    .Build());

            //Act Addition
            Datum result = Engine.CallFunction("sum", new Datum[] { arg });

            //Assert
            result.Should().NotBeNull();

            result.Kind.Should().Be(DatumKind.Scalar);
            result.Scalar.Should().NotBeNull();
            result.Scalar.IsNumeric.Should().BeTrue();
            result.Scalar.ValueType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Double);

            var sum = ((NumericScalar<double>)result.Scalar).Value;

            sum.Should().Be((double)values.Select(x=>(double)x).Sum());
        }
        #endregion

        #region Min/Max
        [Theory]
        [InlineData(new[] { -1 }, -1, -1)]
        [InlineData(new[] { 25, -1, 345, -3, 45, -2  }, -3, 345)]
        [InlineData(new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }, 1, 15)]
        [InlineData(new[] { 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 }, 0, 16)]
        public void Test_AggregationMaxOverInt32(int[] values, int expectedMin, int expectedMax)
        {
            //Arrange
            var arg = new Datum(
                new Int32Array.Builder()
                    .AppendRange(values)
                    .Build());

            //Act Min
            Datum resultMin = Engine.CallFunction("min", new Datum[] { arg });

            //Assert
            resultMin.Should().NotBeNull();

            resultMin.Kind.Should().Be(DatumKind.Scalar);
            resultMin.Scalar.Should().NotBeNull();
            resultMin.Scalar.IsNumeric.Should().BeTrue();
            resultMin.Scalar.ValueType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);

            var min = ((NumericScalar<int>)resultMin.Scalar).Value;
            min.Should().Be(expectedMin);

            //Act Max
            Datum resultMax = Engine.CallFunction("max", new Datum[] { arg });

            //Assert
            resultMax.Should().NotBeNull();

            resultMax.Kind.Should().Be(DatumKind.Scalar);
            resultMax.Scalar.Should().NotBeNull();
            resultMax.Scalar.IsNumeric.Should().BeTrue();
            resultMax.Scalar.ValueType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);

            var max = ((NumericScalar<int>)resultMax.Scalar).Value;
            max.Should().Be(expectedMax);
        }
        #endregion
    }
}
