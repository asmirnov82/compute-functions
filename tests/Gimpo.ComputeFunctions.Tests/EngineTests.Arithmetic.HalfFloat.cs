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
        [Fact]
        public void Arithmetic_Half_Half_WithoutNull_Test()
        {
            var left = new Half[] { (Half)1.0, (Half)(-2.5), (Half)1.0, (Half)1.5, (Half)22, (Half)2.5, (Half)(-3.33), (Half)3.5, (Half)4.0 };
            var right = new Half[] { (Half)2.0, (Half)(-3.5), (Half)1.0, (Half)2.2, (Half)22.6, (Half)2.8, (Half)(-3.66), (Half)3.7, (Half)4.0 };

            //Arrange
            var arg1 = new HalfFloatArray.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new HalfFloatArray.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            HalfFloatArray array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.HalfFloat);
            result.Length.Should().Be(left.Length);

            array = (HalfFloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.HalfFloat);
            result.Length.Should().Be(left.Length);

            array = (HalfFloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.HalfFloat);
            result.Length.Should().Be(left.Length);

            array = (HalfFloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.HalfFloat);
            result.Length.Should().Be(left.Length);

            array = (HalfFloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.HalfFloat);
            result.Length.Should().Be(left.Length);

            array = (HalfFloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Fact]
        public void Arithmetic_Half_Double_WithoutNull_Test()
        {
            var left = new Half[] { (Half)1.0, (Half)(- 2.5), (Half)1.0, (Half)1.5, (Half)22, (Half)2.5, (Half)(- 3.33), (Half)3.5, (Half)4.0 };
            var right = new[] { 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0 };

            //Arrange
            var arg1 = new HalfFloatArray.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new DoubleArray.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            DoubleArray array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Double);
            result.Length.Should().Be(left.Length);

            array = (DoubleArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be((double)left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Double);
            result.Length.Should().Be(left.Length);

            array = (DoubleArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be((double)left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Double);
            result.Length.Should().Be(left.Length);

            array = (DoubleArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be((double)left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Double);
            result.Length.Should().Be(left.Length);

            array = (DoubleArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be((double)left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Double);
            result.Length.Should().Be(left.Length);

            array = (DoubleArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be((double)left[i] % right[i]);
        }

        [Fact]
        public void Arithmetic_Half_Long_WithoutNull_Test()
        {
            var left = new Half[] { (Half)1.0, (Half)(-2.5), (Half)1.0, (Half)1.5, (Half)22, (Half)2.5, (Half)(-3.33), (Half)3.5, (Half)4.0 };
            var right = new long[] { 1, -2, 3, 4, long.MaxValue, 33, long.MinValue, 128, 256 };

            //Arrange
            var arg1 = new HalfFloatArray.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new Int64Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            HalfFloatArray array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.HalfFloat);
            result.Length.Should().Be(left.Length);

            array = (HalfFloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + (Half)right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.HalfFloat);
            result.Length.Should().Be(left.Length);

            array = (HalfFloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - (Half)right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.HalfFloat);
            result.Length.Should().Be(left.Length);

            array = (HalfFloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * (Half)right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.HalfFloat);
            result.Length.Should().Be(left.Length);

            array = (HalfFloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / (Half)right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.HalfFloat);
            result.Length.Should().Be(left.Length);

            array = (HalfFloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % (Half)right[i]);
        }
    }
}
