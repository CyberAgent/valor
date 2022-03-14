---
sidebar_position: 2
---
# Basic Concepts

Valor automatically transforms relational tuples into physical records (e.g., key-values) based on binary schema.
Binary schema ddefines mappings between logical and internal schemas.

![Valor Schema Mapping Model](/img/model.svg)

## Logical schema

Logical schema can be defined as `Relation`.
Similar to popular relational databases, Relation has a set of `Tuples` and is defined with a list of `Attributes`.

## Internal schema

Internal schema specifies how data is physical located.
`Storage` is a top level component of internal schema and describes configurations to access data store. For instance, Storage can includes the type of databases and server address.
`Record` is a unit of data, by whihc storage insert or delete data.
Each record is composed of one or more `Fields`.
Field is a portion of record and is composed of a byte array.

## Binary schema

Binary schema (`Schema`) defines a mapping between logical an dinternal schema as how attribute values are aligned in records and fields.
In binary schema, we can define `Layout` of each field of records. The layout is composed of a list of segment that defines a byte array put into the field.
The segment is composed of two kinds of element, `Formatter` and `Holder`.
The formatter specifies how the byte array is generated and parsed.
For instance, AttributeValueFormatter writes binary expression of a given attribute value and parse the binary as a Java object.
The holder define a boundary with following segment.
As an exmaple of the holdr is a SuffixHolder which attaches a configured byte array at the end of a target segment.


