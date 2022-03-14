---
title: Formatters
---


### Attribute Formatters

## attr

serialize/deserialize an attribute value based on its type

* parameter

|name | type |default| description |
|---|---|---|---|
|attr| string || attribute name |

## currentTime

write the current time and read it as a long attribute value

* parameter

|name | type   | default | description                                                                |
|---|--------|---------|----------------------------------------------------------------------------|
|attr| string |         | long attribute name used to receive the current time |

## datetime2long

convert datetime string to long in write and format the long according to configured date time pattern

* parameter

|name | type |default| description |
|---|---|---|---|
|attr| string || attribute name |
|pattern| string | | datetime string format |
|patterns| array<string\> | | a list of possible datetime string formats |
|order| string | asc | if desc, long value is reversed (Long.MAX_VALUE - value)  |
|timezone | string | | time zone |


## jq

extract object according to configured jq query (currently read only)

|name | type |default| description |
|---|---|---|---|
|jq| string | | jq expression to build object |

## json

read json as object and write object as json


|name | type |default| description |
|---|---|---|---|
|attrs| array<string/> | | a list of attributes included in json |


## filterMap

serialize/deserialize a map which include only configured keys

|name | type |default| description |
|---|---|---|---|
|attr| string | | name of map attribute |
|include | array<string\> | | list of keys includes in this segment |


## map2string

write map as string with configured separators and read the string as a map | |

|name | type |default| description |
|---|---|---|---|
|attr| string | | name of map attribute |

## mapEntry

write a value of a configured key of a map attribute and read the value as entry of a map

|name | type |default| description |
|---|---|---|---|
|attr| string | | name of map attribute |
|key| string | | key of an entry included in this segment|

## number2string

write a number as a string and read the string as a number. If the string contains '.' the value is read as float, otherwise long

|name | type |default| description |
|---|---|---|---|
|attr| string || attribute name |


## reverseLong

put `Long.MAX_VALUE - value` of a long attribute value, and read the value as the original

|name | type |default| description |
|---|---|---|---|
|attr| string || attribute name |

## string2datetimeframe

 write a datetime attribute as 8 bytes encoded datatime frame

A date time frame is

|type (1byte) | year (2byte) |month (1byte)| day (1byte) | hour (1byte) | minute (1byte) | second (1byte) |
|-|-|-|-|-|-|-|


|name | type |default| description |
|---|---|---|---|
|attr| string || attribute name |


## string2number

write a string as a number and read the number as string. If the string contains '.' the value is read as float, otherwise long

|name | type |default| description |
|---|---|---|---|
|attr| string || attribute name |

## urlEncode

write url encoded value of an attribute value and read the encoded value as the original | |

|name | type |default| description |
|---|---|---|---|
|attr| string || attribute name |

## nullableString

write a string attribute value, however, in case of null value, write `0x00`. ||

|name | type |default| description |
|---|---|---|---|
|attr| string || attribute name |

### Hash Formatters

## md5

put md5 hash of configured attributes and skip the hash value

|name | type |default| description |
|---|---|---|---|
|attr| string || attribute name used to compute hash value |
|attrs| array<string/> || a list of attribute names used to compute hash value |
|length| int || the length of the hash value |

## murmur3

 put murmur3 hash (int) of configured attributes and skip the hash value


|name | type |default| description |
|---|---|---|---|
|attr| string || attribute name used to compute hash value |
|attrs| array<string/> || a list of attribute names used to compute hash value |
|length| int || the length of the hash value |

## murmur3mapkey

put murmur3 hash (int) of keys of a map and skip the hash value


|name | type |default| description |
|---|---|---|---|
|mapAttr| string || name of map attribute used to compute hash value |
|length| int || the length of the hash value |


## murmur3salt

distribute records based on murmur3 salt (hash divided　by configured splits size)

|name | type |default| description |
|---|---|---|---|
|attrs| array<string/> || a list of attribute names used to compute hash value |
|range| int || the number of splits |

## sha1

put sha1 hash of configured attributes and skip the hash value

|name | type |default| description |
|---|---|---|---|
|attr| string || attribute name used to compute hash value |
|attrs| array<string/> || a list of attribute names used to compute hash value |
|length| int || the length of the hash value |

### Constant Formatter

## const

put constant value in write and skip the constant in read

* parameter

|name | type |default| description |
|---|---|---|---|
|value| string | | constant value |
|encode| string | | encode method used to convert to byte array |

### Multi Records Formatters

Formatters which split a tuple into multiple records


## mapKey

write keys of a map to separate records　and build map from the records

|name | type |default| description |
|---|---|---|---|
|mapAttr| string | | name of map attribute |

## mapValue

write values of a map to separate records　and build map from the records. This formatter should follow mapKey formatter | one tuple - multi |records |

|name | type |default| description |
|---|---|---|---|
|mapAttr| string | | name of map attribute |

## attrName

put attribute names to separate records and configure a tuple from records

|name | type |default| description |
|---|---|---|---|
|attrName| string | | name of map attribute |

## attrValue

put attribute values to separate records and configure a tuple from records. The formatter should follow attrName formatter


|name | type |default| description |
|---|---|---|---|
|attrName| string | | name of map attribute |
|custom | map<string, formatterSpec/> |  | map to formatter for each value |



