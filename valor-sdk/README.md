valor-sdk
=========

Standand components for valor

List of Components
---

### Schema Repository
|name| description|
|---|---|
|memmory| manage schema in　jvm heap |
|file | manage schema in local file system |
|http | get relation/schema information via http (currently read only)|

### Formatters

|name | description | multiplicity |
|---|---|---|
|attr | serialize/deserialize an attribute value based on its type ||
|const | put constant value in write and skip the constant in read ||
|datetime2long | convert datetime string to long in write and format the long according to configured date time pattern|
|jq | extract object according to configured jq query (currently read only)||
|json| read json as objectn and write object as json | multi-tuples - one record|
|filterMap | serialize/deserialize a map which include only configured keys | |
|map2string | write map as string with configured separators and read the string as a map | |
|mapEntry | write a value of a configured key of a map attribute and read the value as entry of a map ||
|mapKey | write keys of a map to separate records　and build map from the records | one tuple - multi records |
|mapValue | write values of a map to separate records　and build map from the records. This formatter should follow mapKey formatter | one tuple - multi |records |
|md5 | put md5 hash of configured attributes and skip the hash value ||
|attrName | put attribute names to separate records and configure a tuple from records | one tuple - multi records |
|attrValue | put attribute values to separate records and configure a tuple from records. The formatter should follow attrName formatter | one tuple - multi |records |
|murmur3 | put murmur3 hash (int) of configured attributes and skip the hash value ||
|murmur3mapkey | put murmur3 hash (int) of keys of a map and skip the hash value ||
|murmur3salt | distribute records based on murmur3 salt (hash divided　by configured splits size) ||
|number2string | write a number as a string and read the string as a number. If the string contains '.' the value is read as float, otherwise long ||
|reverseLong | put `Long.MAX_VALUE - value` of a long attribute value, and read the value as the original | |
|sha1 | put sha1 hash of configured attributes and skip the hash value ||
|string2datetimeframe | write a datetime attribute as 8 bytes encoded datatime frame ||
|string2number | write a string as a number and read the number as string. If the string contains '.' the value is read as float, otherwise long ||
|urlEncode | write url encoded value of an attribute value and read the encoded value as the original | |
|nullableString |write a string attribute value, however, in case of null value, write `0x00`. ||

### Holder

|name | description |
|---|---|
|fixLength | indicates a segment has fixed length |
|regexp | indicates a segment can be extracted by a regular expression |
|suffix | puts a suffix to a segment |
|size | puts a size of a segment in vint as prefix |

### Storage

|name | physical storage |
|---|---|
| file | local file system|

