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
        [InlineData(new int[] { 0 }, new long[] { 1 })]
        [InlineData(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, new long[] { 1, 2, 3, 4, long.MaxValue, long.MinValue, 44, 128, 256 })]
        public void Arithmetic_Int_Long_WithoutNull_Test(int[] left, long[] right)
        {
            //Arrange
            var arg1 = new Int32Array.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new Int64Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            Int64Array array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new int[] { 0 }, new[] { 1 })]
        [InlineData(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, new int[] { 1, 2, 3, 4, int.MaxValue, int.MinValue, 44, 128, 256 })]
        public void Arithmetic_Int_Int_WithoutNull_Test(int[] left, int[] right)
        {
            //Arrange
            var arg1 = new Int32Array.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new Int32Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            Int32Array array;

            //Act Addition
            result = Engine.Add(arg1, arg2 );

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new int[] { 0 }, new uint[] { 1 })]
        [InlineData(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, new uint[] { 1, 2, 3, 4, uint.MaxValue, 33, 44, 128, 256 })]
        public void Arithmetic_Int_UInt_WithoutNull_Test(int[] left, uint[] right)
        {
            //Arrange
            var arg1 = new Int32Array.Builder()
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
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int64);
            result.Length.Should().Be(left.Length);

            array = (Int64Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new int[] { 0 }, new short[] { 1 })]
        [InlineData(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, new short[] { 1, 2, 3, 4, short.MaxValue, short.MinValue, 44, 128, 256 })]
        public void Arithmetic_Int_Short_WithoutNull_Test(int[] left, short[] right)
        {
            //Arrange
            var arg1 = new Int32Array.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new Int16Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            Int32Array array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new int[] { 0 }, new ushort[] { 1 })]
        [InlineData(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, new ushort[] { 1, 2, 3, 4, ushort.MaxValue, 33, 44, 128, 256 })]
        public void Arithmetic_Int_UShort_WithoutNull_Test(int[] left, ushort[] right)
        {
            //Arrange
            var arg1 = new Int32Array.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new UInt16Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            Int32Array array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Division
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new int[] { 0 }, new sbyte[] { 1 })]
        [InlineData(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, new sbyte[] { 1, 2, 3, 4, sbyte.MaxValue, sbyte.MinValue, 44, 64, 68 })]
        public void Arithmetic_Int_Sbyte_WithoutNull_Test(int[] left, sbyte[] right)
        {
            //Arrange
            var arg1 = new Int32Array.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new Int8Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            Int32Array array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new int[] { 0 }, new byte[] { 1 })]
        [InlineData(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, new byte[] { 1, 2, 3, 4, byte.MaxValue, 33, 44, 64, 128})]
        public void Arithmetic_Int_Byte_WithoutNull_Test(int[] left, byte[] right)
        {
            //Arrange
            var arg1 = new Int32Array.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new UInt8Array.Builder()
                .AppendRange(right)
                .Build();

            IArrowArray result;
            Int32Array array;

            //Act Addition
            result = Engine.Add(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] + right[i]);

            //Act Subtraction
            result = Engine.Subtract(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] - right[i]);

            //Act Multiplication
            result = Engine.Multiply(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] * right[i]);

            //Act Division
            result = Engine.Divide(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] / right[i]);

            //Act Modulo
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Should().NotBeNull();
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Int32);
            result.Length.Should().Be(left.Length);

            array = (Int32Array)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }


        [Theory]
        [InlineData(new int [] { 0 }, new double[] { 1.0 })]
        [InlineData(new int[] { 1, -2, 3, 4, int.MaxValue, 33, int.MinValue, 128, 256 }, new[] { 1.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0 })]
        public void Arithmetic_Int_Double_WithoutNull_Test(int[] left, double[] right)
        {
            //Arrange
            var arg1 = new Int32Array.Builder()
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

            //Act Division
            result = Engine.Modulo(arg1, arg2);

            //Assert
            result.Data.DataType.TypeId.Should().Be(Apache.Arrow.Types.ArrowTypeId.Double);
            result.Length.Should().Be(left.Length);

            array = (DoubleArray)result;

            for (int i = 0; i < array.Length; i++)
                array.Values[i].Should().Be(left[i] % right[i]);
        }

        [Theory]
        [InlineData(new int[] { 0 }, new[] { 1.0f })]
        [InlineData(new int[] { 1, -2, 3, 4, int.MaxValue, 33, int.MinValue, 128, 256 }, new[] { 1.0f, 0.5f, 1.0f, 1.5f, 2.0f, 2.5f, 3.0f, 3.5f, 4.0f })]
        public void Arithmetic_Int_Float_WithoutNull_Test(int[] left, float[] right)
        {
            //Arrange
            var arg1 = new Int32Array.Builder()
                .AppendRange(left)
                .Build();

            var arg2 = new FloatArray.Builder()
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
