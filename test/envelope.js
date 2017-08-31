"use strict";
var parquet = require('..');

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

// inspect the output file with
// $ hadoop jar parquet-tools-1.9.0.jar meta test.parquet
// $ hadoop jar parquet-tools-1.9.0.jar dump test.parquet
