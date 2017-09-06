'use strict';
const parquet = require('..');

// write a new file 'fruits.parquet'
let schema = new parquet.ParquetSchema({
  name:       { type: 'UTF8' },
  quantity:   { type: 'INT64', optional: true },
  price:      { type: 'DOUBLE' },
  date:       { type: 'TIMESTAMP_MICROS' },
  in_stock:   { type: 'BOOLEAN' },
  colour:     { type: 'UTF8', repeated: true },
  meta_json:  { type: 'BSON', optional: true  },
});

let writer = parquet.ParquetWriter.openFile(schema, 'fruits.parquet');

writer.appendRow({
  name: 'apples',
  quantity: 10,
  price: 2.6,
  date: new Date(),
  in_stock: true,
  colour: [ 'green', 'red' ]
});

writer.appendRow({
  name: 'oranges',
  quantity: 20,
  price: 2.7,
  date: new Date(),
  in_stock: true,
  colour: [ 'orange' ]
});

writer.appendRow({
  name: 'kiwi',
  price: 4.2,
  date: new Date(),
  in_stock: false,
  colour: [ 'green', 'brown' ],
  meta_json: { expected_ship_date: new Date() }
});

writer.close();

// inspect the output file with
// $ hadoop jar parquet-tools-1.9.0.jar meta fruits.parquet
// $ hadoop jar parquet-tools-1.9.0.jar dump fruits.parquet
