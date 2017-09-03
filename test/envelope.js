"use strict";
var parquet = require('..');

var schema = new parquet.ParquetSchema();
schema.addColumn({name: "name", type: "BYTE_ARRAY"});
schema.addColumn({name: "quantity", type: "INT64"});
schema.addColumn({name: "price", type: "DOUBLE"});
schema.addColumn({name: "date", type: "INT64"});
schema.addColumn({name: "in_stock", type: "BOOLEAN"});

var writer = new parquet.ParquetFileWriter(schema, 'test.parquet');
for (let i = 0; i < 32; ++i) {
  writer.appendRow({name: 'apples', quantity: i, price: 2.5, date: +new Date(), in_stock: true});
  writer.appendRow({name: 'oranges', quantity: i * 2, price: 2.5, date: +new Date(), in_stock: false});
}
writer.end();

// inspect the output file with
// $ hadoop jar parquet-tools-1.9.0.jar meta test.parquet
// $ hadoop jar parquet-tools-1.9.0.jar dump test.parquet
