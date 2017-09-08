'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('..');

// FIXME: tempdir?
// write a new file 'fruits.parquet'
async function writeTestFile() {
  let schema = new parquet.ParquetSchema({
    name:       { type: 'UTF8' },
    quantity:   { type: 'INT64', optional: true },
    price:      { type: 'DOUBLE' },
    date:       { type: 'TIMESTAMP_MICROS' },
    in_stock:   { type: 'BOOLEAN' },
    colour:     { type: 'UTF8', repeated: true },
    meta_json:  { type: 'BSON', optional: true  },
  });

  let writer = await parquet.ParquetWriter.openFile(schema, 'fruits.parquet');

  for (let i = 0; i < 10000; ++i) {
    await writer.appendRow({
      name: 'apples',
      quantity: 10,
      price: 2.6,
      date: new Date(),
      in_stock: true,
      colour: [ 'green', 'red' ]
    });

    await writer.appendRow({
      name: 'oranges',
      quantity: 20,
      price: 2.7,
      date: new Date(),
      in_stock: true,
      colour: [ 'orange' ]
    });

    await writer.appendRow({
      name: 'kiwi',
      price: 4.2,
      date: new Date(),
      in_stock: false,
      colour: [ 'green', 'brown' ],
      meta_json: { expected_ship_date: new Date() }
    });

    await writer.appendRow({
      name: 'banana',
      price: 3.2,
      date: new Date(),
      in_stock: false,
      colour: [ 'yellow' ],
      meta_json: { shape: "curved" }
    });
  }

  await writer.close();
}

async function readTestFile() {
  let reader = await parquet.ParquetReader.openFile('fruits.parquet');
  assert.equal(reader.getRowCount(), 40000);
  reader.close();
}

async function performTest() {
  await writeTestFile();
  await readTestFile();
}

performTest().catch(function(err) {
  console.error(err);
  process.exit(1);
});

