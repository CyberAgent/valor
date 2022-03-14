---
title: Holders
---

## fixLength

indicates a segment has fixed length

## regexp

indicates a segment can be extracted by a regular expression

|name | type |default| description |
|---|---|---|---|
|regexp| string || segment pattern in regular expression |

## suffix

puts a suffix to a segment

|name | type |default| description |
|---|---|---|---|
|suffix| string || suffix value |
|fromHead| boolean |true| find suffix form head or tail |

## size

puts a size of a segment in vint as prefix
