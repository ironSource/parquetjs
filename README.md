# parquet.js

This package contains a fully asynchronous, pure JavaScript implementation of
the [Parquet](https://parquet.apache.org/) file format. The implementation conforms with the
[Parquet specification](https://github.com/apache/parquet-format) and is tested
for compatibility with Apache's Java [reference implementation](https://github.com/apache/parquet-mr).

**What is Parquet?**: Parquet is a column-oriented file format; it allows you to
write a large amount of structured data to a file, compress it and then read parts
of it back out efficiently. The Parquet format is based on [Google's Dremel paper](https://www.google.co.nz/url?sa=t&rct=j&q=&esrc=s&source=web&cd=2&cad=rja&uact=8&ved=0ahUKEwj_tJelpv3UAhUCm5QKHfJODhUQFggsMAE&url=http%3A%2F%2Fwww.vldb.org%2Fpvldb%2Fvldb2010%2Fpapers%2FR29.pdf&usg=AFQjCNGyMk3_JltVZjMahP6LPmqMzYdCkw).


Usage
-----

Here is the full example to write out our 'fruits.parquet' file:

``` js
var parquet = require('parquetjs');

var schema = new parquet.ParquetSchema({
  "name": { type: "STRING" },
  "quantity": { type: "INT64" },
  "price": { type: "DOUBLE" },
  "date": { type: "TIMESTAMP" },
  "in_stock": { type: "BOOLEAN" }
});

var writer = new parquet.ParquetFileWriter(schema, 'test.parquet');
writer.appendRow({name: 'apples', quantity: 10, price: 2.5, date: +new Date(), in_stock: true});
writer.appendRow({name: 'oranges', quantity: 10, price: 2.5, date: +new Date(), in_stock: true});
writer.end();
```


Supported Types & Encodings
---------------------------

We aim to be feature-complete and add new features as they are added to the
parquet specification; this is the list of currently implemented data types and
encodings:

<table>
  <tr><th>Logical Type</th><th>Primitive Type</th><th>Encodings</th></tr>
  <tr><td>UTF8</td><td>BYTE_ARRAY</td><td>PLAIN</td></tr>
  <tr><td>JSON</td><td>BYTE_ARRAY</td><td>PLAIN</td></tr>
  <tr><td>BSON</td><td>BYTE_ARRAY</td><td>PLAIN</td></tr>
  <tr><td>BYTE_ARRAY</td><td>BYTE_ARRAY</td><td>PLAIN</td></tr>
  <tr><td>TIME_MILLIS</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>TIME_MICROS</td><td>INT64</td><td>PLAIN, RLE</td></tr>
  <tr><td>TIMESTAMP_MILLIS</td><td>INT64</td><td>PLAIN, RLE</td></tr>
  <tr><td>TIMESTAMP_MICROS</td><td>INT64</td><td>PLAIN, RLE</td></tr>
  <tr><td>BOOLEAN</td><td>BOOLEAN</td><td>PLAIN, RLE</td></tr>
  <tr><td>FLOAT</td><td>FLOAT</td><td>PLAIN</td></tr>
  <tr><td>DOUBLE</td><td>DOUBLE</td><td>PLAIN</td></tr>
  <tr><td>INT32</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT64</td><td>INT64</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT96</td><td>INT96</td><td>PLAIN</td></tr>
  <tr><td>INT_8</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT_16</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT_32</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT_64</td><td>INT64</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT_8</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT_16</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT_32</td><td>INT32</td><td>PLAIN, RLE</td></tr>
  <tr><td>INT_64</td><td>INT64</td><td>PLAIN, RLE</td></tr>
</table>


Depdendencies
-------------

Parquet uses [thrift](https://thrift.apache.org/) (basically facebook's version of
[protocol buffers](https://developers.google.com/protocol-buffers/)) to encode the schema
and other metadata, but the actual data does not use thrift.


License
-------

Copyright (c) 2017 ironSource Ltd.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in the
Software without restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

