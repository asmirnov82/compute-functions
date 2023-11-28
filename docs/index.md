## The generic Compute API
Functions represent compute operations over inputs of possibly varying types. 
Functions are stored in a global registry inside 'Gimpo.ComputeFunctions.Engine' class and can be looked up by name.

For example:
'''C# IFunction function = Engine.GetFunctionByName("add");'''

## Common numeric type

The common numeric type of a set of input numeric types is the smallest numeric type which can accommodate any value of any input. If any input is a floating point type the common numeric type is the widest floating point type among the inputs. Otherwise the common numeric type is integral and is signed if any input is signed. 

For example:

| Input types       | Common numeric type  |
|-------------------|----------------------|
| int64, int64      | int64                |
| int64, int32      | int64                |
| int64, uint32     | int64                |
| int64, int16      | int64                |
| int64, uint16     | int64                |
| uint64, uint64    | uint64               |
| int32, int32      | int32                |
| int32, uint32     | int64                |
| int32, int16      | int32                |
| int32, uint16     | int32                |
| uint32, uint32    | uint32               |
| uint32, int16     | int64                |
| uint32, uint16    | uint32               |
| int16, int16      | int16                |
| int16, uint16     | int32                |
| double, int32     | double               |
| double, float     | double               |
| double, Half      | double               |
| float,  int32     | float                |
| float, double     | double               |
| float, float      | float                |
| float, Half       | float                |
| Half, Half        | Half                 |

