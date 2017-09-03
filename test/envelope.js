"use strict";
var parquet = require('..');

var schema = new parquet.ParquetSchema({
  name:     { type: "BYTE_ARRAY" },
  quantity: { type: "INT64", optional: true },
  price:    { type: "DOUBLE" },
  date:     { type: "INT64" },
  in_stock: { type: "BOOLEAN" }
});

var writer = new parquet.ParquetFileWriter(schema, 'test.parquet');
for (let i = 0; i < 4; ++i) {
  writer.appendRow({name: 'apples', quantity: 10 + i, price: 2.5, date: +new Date(), in_stock: true});
  writer.appendRow({name: 'oranges', quantity: i * 2, price: 2.5, date: +new Date(), in_stock: true});
  writer.appendRow({name: 'kiwi', price: 4.2, date: +new Date(), in_stock: false});
}
writer.end();

// inspect the output file with
// $ hadoop jar parquet-tools-1.9.0.jar meta test.parquet
// $ hadoop jar parquet-tools-1.9.0.jar dump test.parquet
