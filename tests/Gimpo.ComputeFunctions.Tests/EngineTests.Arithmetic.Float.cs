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
        [InlineData(new[] { 0.0f }, new[] { 1.0f })]
        [InlineData(new[] { 0.0f, 0.5f, 1.0f, 1.5f, 2.0f, 2.5f, 3.0f, 3.5f, 4.0f }, new[] { 1.0f, -2.5f, 1.0f, 1.5f, 22f, 2.5f, -3.33f, 3.5f, 4.0f })]
        public void Arithmetic_Float_Float_WithoutNull_Test(float[] left, float[] right)
        {
            //Arrange
            var arg1 = new FloatArray.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new FloatArray.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            FloatArray array;

            //Act Addition
            result = Engine.Add( arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract( arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply( arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide( arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);
        }

        [Theory]
        [InlineData(new[] { 1.0f }, new[] { 0.0 })]
        [InlineData(new[] { 1.0f, -2.5f, 1.0f, 1.5f, 22f, 2.5f, -3.33f, 3.5f, 4.0f }, new[] { 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0 })]
        public void Arithmetic_Float_Double_WithoutNull_Test(float[] left, double[] right)
        {
            //Arrange
            var arg1 = new FloatArray.Builder()
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
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Double);
            result.Length.Should().Be(left.Length);

            array = (DoubleArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Double);
            result.Length.Should().Be(left.Length);

            array = (DoubleArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Double);
            result.Length.Should().Be(left.Length);

            array = (DoubleArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Double);
            result.Length.Should().Be(left.Length);

            array = (DoubleArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new[] { 0.0f }, new long[] { 1 })]
        [InlineData(new[] { 0.0f, 0.5f, 1.0f, 1.5f, 2.0f, 2.5f, 3.0f, 3.5f, 4.0f }, new long[] { 1, -2, 3, 4, long.MaxValue, 33, long.MinValue, 128, 256 })]
        public void Arithmetic_Float_Long_WithoutNull_Test(float[] left, long[] right)
        {
            //Arrange
            var arg1 = new FloatArray.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new Int64Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            FloatArray array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new[] { 0.0f }, new ulong[] { 1 })]
        [InlineData(new[] { 0.0f, 0.5f, 1.0f, 1.5f, 2.0f, 2.5f, 3.0f, 3.5f, 4.0f }, new ulong[] { 1, 2, 3, 4, ulong.MaxValue, 33, 44, 128, 256 })]
        public void Arithmetic_Float_ULong_WithoutNull_Test(float[] left, ulong[] right)
        {
            //Arrange
            var arg1 = new FloatArray.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new UInt64Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            FloatArray array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new[] { 0.0f }, new int[] { 1 })]
        [InlineData(new[] { 0.0f, 0.5f, 1.0f, 1.5f, 2.0f, 2.5f, 3.0f, 3.5f, 4.0f }, new int[] { 1, -2, 3, 4, int.MaxValue, 33, int.MinValue, 128, 256 })]
        public void Arithmetic_Float_Int_WithoutNull_Test(float[] left, int[] right)
        {
            //Arrange
            var arg1 = new FloatArray.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new Int32Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            FloatArray array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new[] { 0.0f }, new uint[] { 1 })]
        [InlineData(new[] { 0.0f, 0.5f, 1.0f, 1.5f, 2.0f, 2.5f, 3.0f, 3.5f, 4.0f }, new uint[] { 1, 2, 3, 4, uint.MaxValue, 33, 44, 128, 256 })]
        public void Arithmetic_Float_UInt_WithoutNull_Test(float[] left, uint[] right)
        {
            //Arrange
            var arg1 = new FloatArray.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new UInt32Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            FloatArray array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new[] { 0.0f }, new short[] { 1 })]
        [InlineData(new[] { 0.0f, 0.5f, 1.0f, 1.5f, 2.0f, 2.5f, 3.0f, 3.5f, 4.0f }, new short[] { 1, -2, 3, 4, short.MaxValue, 33, short.MinValue, 128, 256 })]
        public void Arithmetic_Float_Int16_WithoutNull_Test(float[] left, short[] right)
        {
            //Arrange
            var arg1 = new FloatArray.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new Int16Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            FloatArray array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new[] { 0.0f }, new ushort[] { 1 })]
        [InlineData(new[] { 0.0f, 0.5f, 1.0f, 1.5f, 2.0f, 2.5f, 3.0f, 3.5f, 4.0f }, new ushort[] { 1, 2, 3, 4, ushort.MaxValue, 33, 44, 128, 256 })]
        public void Arithmetic_Float_UInt16_WithoutNull_Test(float[] left, ushort[] right)
        {
            //Arrange
            var arg1 = new FloatArray.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new UInt16Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            FloatArray array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new[] { 0.0f }, new sbyte[] { 1 })]
        [InlineData(new[] { 0.0f, 0.5f, 1.0f, 1.5f, 2.0f, 2.5f, 3.0f, 3.5f, 4.0f }, new sbyte[] { 1, -2, 3, 4, sbyte.MaxValue, 33, sbyte.MinValue, 32, 64 })]
        public void Arithmetic_Float_Int8_WithoutNull_Test(float[] left, sbyte[] right)
        {
            //Arrange
            var arg1 = new FloatArray.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new Int8Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            FloatArray array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new[] { 0.0f }, new byte[] { 1 })]
        [InlineData(new[] { 0.0f, 0.5f, 1.0f, 1.5f, 2.0f, 2.5f, 3.0f, 3.5f, 4.0f }, new byte[] { 1, 2, 3, 4, byte.MaxValue, 33, 44, 32, 64 })]
        public void Arithmetic_Float_UInt8_WithoutNull_Test(float[] left, byte[] right)
        {
            //Arrange
            var arg1 = new FloatArray.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new UInt8Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            FloatArray array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Float);
            result.Length.Should().Be(left.Length);

            array = (FloatArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }
    }
}
