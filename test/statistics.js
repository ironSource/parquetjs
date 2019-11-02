'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('../parquet.js');
const TEST_VTIME =  new Date();

const schema = new parquet.ParquetSchema({
  name:       { type: 'UTF8' },
  //quantity:   { type: 'INT64', encoding: 'RLE', typeLength: 6, optional: true }, // parquet-mr actually doesnt support this
  quantity:   { type: 'INT64', optional: true },
  price:      { type: 'DOUBLE' },
  date:       { type: 'TIMESTAMP_MICROS' },
  day:        { type: 'DATE' },
  finger:     { type: 'FIXED_LEN_BYTE_ARRAY', typeLength: 5 },
  inter:      { type: 'INTERVAL', statistics: false },
  stock: {
    repeated: true,
    fields: {
      quantity: { type: 'INT64', repeated: true },
      warehouse: { type: 'UTF8' },
    }
  },
  colour:     { type: 'UTF8', repeated: true },
  meta_json:  { type: 'BSON', optional: true, statistics: false},
});


describe('statistics', async function() {
  before(async function(){
    let writer = await parquet.ParquetWriter.openFile(schema, 'fruits-statistics.parquet', {pageSize: 3});
    
    writer.appendRow({
      name: 'apples',
      quantity: 10,
      price: 2.6,
      day: new Date('2017-11-26'),
      date: new Date(TEST_VTIME + 1000),
      finger: "FNORD",
      inter: { months: 10, days: 5, milliseconds: 777 },
      stock: [
        { quantity: 10, warehouse: "A" },
        { quantity: 20, warehouse: "B" }
      ],
      colour: [ 'green', 'red' ]
    });

    writer.appendRow({
      name: 'oranges',
      quantity: 20,
      price: 2.7,
      day: new Date('2018-03-03'),
      date: new Date(TEST_VTIME + 2000),
      finger: "ABCDE",
      inter: { months: 42, days: 23, milliseconds: 777 },
      stock: {
        quantity: [50, 33, 34, 35, 36],
        warehouse: "X"
      },
      colour: [ 'orange' ]
    });

    writer.appendRow({
      name: 'kiwi',
      price: 4.2,
      quantity: 15,
      day: new Date('2008-11-26'),
      date: new Date(TEST_VTIME + 8000),
      finger: "XCVBN",
      inter: { months: 60, days: 1, milliseconds: 99 },
      stock: [
        { quantity: 42, warehouse: "f" },
        { quantity: 21, warehouse: "x" }
      ],
      colour: [ 'green', 'brown', 'yellow' ],
      meta_json: { expected_ship_date: TEST_VTIME }
    });

    writer.appendRow({
      name: 'banana',
      price: 3.2,
      day: new Date('2017-11-26'),
      date: new Date(TEST_VTIME + 6000),
      finger: "FNORD",
      inter: { months: 1, days: 15, milliseconds: 888 },
      colour: [ 'yellow'],
      meta_json: { shape: 'curved' }
    });

    await writer.close();
  });

  it('column statistics should match input', async function() {
    let reader = await parquet.ParquetReader.openFile('fruits-statistics.parquet');
    const row = reader.metadata.row_groups[0];
    const rowStats = (path) => row.columns.find(d => d.meta_data.path_in_schema.join(',') == path).meta_data.statistics;

    assert.equal(rowStats('name').min_value,'apples');
    assert.equal(rowStats('name').max_value,'oranges');
    assert.equal(+rowStats('name').distinct_count,4);
    assert.equal(+rowStats('name').null_count,0);

    assert.equal(rowStats('quantity').min_value,10);
    assert.equal(rowStats('quantity').max_value,20);
    assert.equal(+rowStats('quantity').distinct_count,3);
    assert.equal(+rowStats('quantity').null_count,1);

    assert.equal(rowStats('price').min_value, 2.6);
    assert.equal(rowStats('price').max_value, 4.2);
    assert.equal(+rowStats('price').distinct_count, 4);
    assert.equal(+rowStats('price').null_count, 0);
    
    assert.deepEqual(rowStats('day').min_value, new Date('2008-11-26'));
    assert.deepEqual(rowStats('day').max_value, new Date('2018-03-03'));
    assert.equal(+rowStats('day').distinct_count, 4);
    assert.equal(+rowStats('day').null_count, 0);

    assert.deepEqual(rowStats('finger').min_value, 'ABCDE');
    assert.deepEqual(rowStats('finger').max_value, 'XCVBN');
    assert.equal(+rowStats('finger').distinct_count, 3);
    assert.equal(+rowStats('finger').null_count, 0);

    assert.deepEqual(rowStats('stock,quantity').min_value, 10);
    assert.deepEqual(rowStats('stock,quantity').max_value, 50);
    assert.equal(+rowStats('stock,quantity').distinct_count, 9);
    assert.equal(+rowStats('stock,quantity').null_count, 1);

    assert.deepEqual(rowStats('stock,warehouse').min_value, 'A');
    assert.deepEqual(rowStats('stock,warehouse').max_value, 'x');
    assert.equal(+rowStats('stock,warehouse').distinct_count, 5);
    assert.equal(+rowStats('stock,warehouse').null_count, 1);

    assert.deepEqual(rowStats('colour').min_value, 'brown');
    assert.deepEqual(rowStats('colour').max_value, 'yellow');
    assert.equal(+rowStats('colour').distinct_count, 5);
    assert.equal(+rowStats('colour').null_count, 0);

    assert.equal(rowStats('inter'), null);
    assert.equal(rowStats('meta_json'), null);
  });
});