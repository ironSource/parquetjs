# parquet.js

encode/decode parquet files from node/javascript

## example

``` js
var createEncoder = require('parquet.js/encode')

//initialize encoder with {<name>: <type>,} for example.
var encodeRows = createEncoder({
  name: 'string',
  quantity: 'int',
  price: 'double',
  date: 'timestamp',
  in_stock: 'boolean'
})
//btw, order is significant!

//then pass batches of rows to that, you want the batch to be pretty big
//like, 10k or 20k rows. will write a whole `row_group` which you want to be
//at least 20 mb

encodeRows([
  {name: 'apples', quantity: 10, price: 2.5, date: +new Date(), in_stock: true},
  //...etc
]) => buffer

//finally, call encodeRows with no args to finish off the file.
//the footer information which is essential to interpreting the file
//so without this the parquet file is invalid.
encodeRows() => footer
```

## all about parquet

Parquet is a column oriented file format. The idea is that you can write a large amount
of structured data to a file and then read parts of it back out efficiently.
It is based on ideas from google's [dremel paper](https://www.google.co.nz/url?sa=t&rct=j&q=&esrc=s&source=web&cd=2&cad=rja&uact=8&ved=0ahUKEwj_tJelpv3UAhUCm5QKHfJODhUQFggsMAE&url=http%3A%2F%2Fwww.vldb.org%2Fpvldb%2Fvldb2010%2Fpapers%2FR29.pdf&usg=AFQjCNGyMk3_JltVZjMahP6LPmqMzYdCkw)
but was created by twitter and facebook to be compatible with hadoop.

Parquet takes a schema, but can represent nested data like json, but not _arbitary_ json,
for example, you couldn`t have structure of recursively nested arrays - you'd need a schema that
specified some fixed level of maximum nesting and so you'd always be able to create a value
that exceded that.

Parquet uses [thrift](https://thrift.apache.org/) (basically facebook's version of
[protocol buffers](https://developers.google.com/protocol-buffers/)) to encode the schema
and other metadata, but the actual data does not use thrift.

Parquet uses a variety of encodings to represent values, each optimized for a particular type
of data. values are written a column at time (known as a "column chunk") in groups of say 20k rows
(known as a "row group"). Contrasting
with CSV, which stores data a row at a time, this writes out data from a bunch of  rows (a row group), one column
then the next (column chunks). Writing column chunks at a time allows better compression, as the data inside a column
is likely similar, and writing rows groups allows you to be still somewhat streaming (it's
not necessary to keep the whole file in memory, just a row group and the metadata for the row groups that have been written)

## data types

there is a small number of basic data types

* `byte-array` - aka text.
* `int32`, `int64` - integers
* `double`, `float` - numbers
* `int96` - 12 byte numbers used for timestamps
* `boolean`

all numbers are encoded little endian as this is faster on most hardware.

## definition level and repetition level

Definition level shows the level in the (nested) schema of each piece of data.
Since I only needed to deal with flat csv style data, this hasn't been implemented.

Repetition level allows nulls to be skipped, and is implemented. (it also allows arrays,
as values, which hasn't been implemented)

Each column chunk

## encodings

The various encodings from simplest to complexest

### plain

Just the raw data items one after another with nothing clever.
for `byte-array` each string is proceded by a int32 length.
other values are written back to back at their full bit widths.
The most complicated is `boolean` which is written as one bit per value.

### hybrid run length encoding / bit packing

bit packing encodes a series of numbers at a given bit width, so if the biggest number
in a column chunk was 2713 that column can be encoded using just 12 bits.

run length encoding works well if there are lots of repeated values, just the value
is encoded as the value (in the bit width rounded up to whole bytes - eg, 12 bits as 2 bytes),
then a varint number of repeats. If an entire column is only one value, it will be encoded
as just a few bytes.

The "hybrid" part, is that it switches between bitpacked and rle on the fly, if the value is
not repeated the next 8 values are written bitpacked (8, because then you'll always use whole bytes)

### dictionary

for strings (aka `byte-arrays`) that have lots of repeated values dictionary works well.
first the unique values are written using plain encoding, then the indexes of those values
are written using hybrid encoding.

### delta encodings

othe clever stuff for stings, not implemented.





## BRAINDUMP

in this repo you'll find

* some brutal code that dumps the metadata of a parquet file (the portion described in parquet.thrift)
* encodings
  * encodings/bitpacking (bitpacking codec)
  * encodings/hybrid (hybrid bitpacking run length encoding)
* tools
  * csv-to-parquet (wrapper for apache-drill that outputs a valid parquet file)

## next steps

generate a parquet file with plain encoding.
for a proof of concept, I'll start with a single column,
then add more columns.

### api

the api will probably look something like this:

``` js
var parquet = require('parquetjs')

var p = parquet(schema, function () {
  //stream passed every row, since it's easiest to buffer
  //rows as they are created, but we need to write them 1 column at a time
  //to parquet file (since it's collumn oriented) that either means
  //that we scan it N times for N columns or otherwise buffer the entire thing.
  return createRowStream()
})
```

schema will be properties and types

```
BOOLEAN: Bit Packed, LSB first
INT32: 4 bytes little endian
INT64: 8 bytes little endian
INT96: 12 bytes little endian
FLOAT: 4 bytes IEEE little endian
DOUBLE: 8 bytes IEEE little endian
BYTE_ARRAY: length in 4 bytes little endian followed by the bytes contained in the array
FIXED_LEN_BYTE_ARRAY: the bytes contained in the array
```

note, parquet supports nested data, but for simplicity we'll stick to flat data for now.

## License

MIT











