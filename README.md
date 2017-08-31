# parquet.js

Read and write parquet files from node.js

## What is Parquet?

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

## Example: Write a parquet file

``` js
var parquet = require('parquetjs');

var schema = new parquet.ParquetSchema();
schema.addColumn({name: "name", type: "STRING"});
schema.addColumn({name: "quantity", type: "INT64"});
schema.addColumn({name: "price", type: "DOUBLE"});
schema.addColumn({name: "date", type: "TIMESTAMP"});
schema.addColumn({name: "in_stock", type: "BOOLEAN"});

var writer = new parquet.ParquetFileWriter(schema, 'test.parquet');
writer.appendRow({name: 'apples', quantity: 10, price: 2.5, date: +new Date(), in_stock: true});
writer.appendRow({name: 'oranges', quantity: 10, price: 2.5, date: +new Date(), in_stock: true});
writer.end();
```

## API Documentation

[ API Documentation here ]

