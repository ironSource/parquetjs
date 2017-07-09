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





