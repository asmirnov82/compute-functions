# Gimpo Compute Functions

An implementation of Arrow Compute Functions targeting .NET 7.0 and .NET 8.0.

Gimpo Compute Functions provide high performance generic Compute API over large data sets, that are compatible with Apache Arrow in-memory columnar format. Most of the operations are vectorized using SIMD intructions of modern CPUs. 

Standard Apache Arrow library provides [C++](https://arrow.apache.org/docs/cpp/compute.html) and [Python](https://arrow.apache.org/docs/python/compute.html) implementations. The aim of this project is to develop .Net version of the API. 


## The generic Compute API
Functions represent compute operations over inputs of possibly varying types. 
Functions are stored in a global registry inside `Gimpo.ComputeFunctions.Engine` class and can be looked up by name.

For example:
```C#
IFunction function = Engine.GetFunctionByName("add");
```

## Invoking functions

Compute functions can be invoked by name using `public static Datum CallFunction(string functionName, IReadOnlyList<Datum> args)` method of `Gimpo.ComputeFunctions.Engine` or by getting ot from a global registry first.

For example:
```C#
//Invoking by name
var arg1 = new Datum(
    new Int32Array.Builder()
        .AppendRange(new[] { 0, 1, 2})
        .Build());

var arg2 = new Datum(
    new Int32Array.Builder()
        .AppendRange(new[] { 2, 1, 0 })
        .Build());

Datum result = Engine.CallFunction("add", new Datum[] {arg1, arg2});

//Function lookup
IFunction function = Engine.GetFunctionByName("add");

result = function.Execute(new Datum[] {arg1, arg2})
```

Many functions are also available directly as concrete APIs. 

For example, 'Gimpo.ComputeFunctions.Engine.Add':
```C#
var array1 = new Int32Array.Builder()
                .AppendRange(new[] { 0, 1, 2 })
                .Build();

var array2 = new Int32Array.Builder()
                .AppendRange(new[] { 0, 2, 1 })
                .Build();

IArrowArray = Engine.Add(array1, array2);
```
or
```C#
var array1 = new Int32Array.Builder()
                .AppendRange(new[] { 0, 1, 2 })
                .Build();

var scalar = Scalar.Create(32)

IArrowArray = Engine.Add(array1, scalar);
```

## Common numeric type

The common numeric type of a set of input numeric types is the smallest numeric type which can accommodate any value of any input. If any input is a floating point type the common numeric type is the widest floating point type among the inputs. Otherwise the common numeric type is integral and is signed if any input is signed. 

For example:
| Input types       | Common numeric type  |
|-------------------|----------------------|
| int64, int64      | int64                |
| int64, int32      | int64                |
| int64, int16      | int64                |
| int32, int32      | int32                |
| int32, uint32     | int64                |
| uint32, int16     | int64                |
| double, int32     | double               |
| double, float     | double               |
| float,  int32     | float                |
| float, double     | double               |
| float, float      | float                |

## Element-wise binary functions

All element-wise binary functions accept both arrays and scalars as input.

Binary functions have the following semantics (which is sometimes called “broadcasting” in other systems such as Pandas):

*(array, array)* inputs produce an array output (and both inputs must be of the same length)

*(scalar, array)* and *(array, scalar)* produce an array output. The scalar input is handled as if it were an array of the same length N as the other input, with the same value repeated N times.