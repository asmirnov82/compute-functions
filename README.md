# Introduction

## Gimpo Compute Functions

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

Several types of functions are currently supported:

1. Aggregations (operate on a array and reduce the input to a single output value)

2. Element-wise functions (accept both arrays and scalars as input and produce array of the same length as an output in case of at least on if the input parameter is a scalar or a single output value in case of all input parameters are scalars)

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

# Functions

## Aggregations

Scalar aggregations operate on an array and reduce the input to a single output value. The type of the output depends on Aggregation type. Most of the functions (like *min* and *max*) return the same type as the input type. Others (like *sum*) widen the type, so it's *int64* for all signed integers, *uint64* for all unsigned integers and *double* for all floating numbers (like *double*, *float* and *Half*).

## Element-wise arithmetic functions

All element-wise functions accept both arrays and scalars as input. These have the following semantics (which is sometimes called “broadcasting” in other systems such as Pandas):

*(scalar, scalar)* inputs produce a scalar output (not implemented yet)

*(array, array)* inputs produce an array output (and both inputs must be of the same length)

*(scalar, array)* and *(array, scalar)* produce an array output. The scalar input is handled as if it were an array of the same length N as the other input, with the same value repeated N times.

Arithmetic functions expect inputs of numeric type and apply a given arithmetic operation to each element(s) gathered from the input(s). If any of the input element(s) is null, the corresponding output element is null.