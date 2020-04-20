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
  let row, reader;

  before(async function(){
    let writer = await parquet.ParquetWriter.openFile(schema, 'fruits-statistics.parquet', {pageSize: 3});
    
    writer.appendRow({
      name: 'apples',
      quantity: 10n,
      price: 2.6,
      day: new Date('2017-11-26'),
      date: new Date(TEST_VTIME + 1000),
      finger: "FNORD",
      inter: { months: 10, days: 5, milliseconds: 777 },
      stock: [
        { quantity: 10n, warehouse: "A" },
        { quantity: 20n, warehouse: "B" }
      ],
      colour: [ 'green', 'red' ]
    });

    writer.appendRow({
      name: 'oranges',
      quantity: 20n,
      price: 2.7,
      day: new Date('2018-03-03'),
      date: new Date(TEST_VTIME + 2000),
      finger: "ABCDE",
      inter: { months: 42, days: 23, milliseconds: 777 },
      stock: {
        quantity: [50n, 33n, 34n, 35n, 36n],
        warehouse: "X"
      },
      colour: [ 'orange' ]
    });

    writer.appendRow({
      name: 'kiwi',
      price: 4.2,
      quantity: 15n,
      day: new Date('2008-11-26'),
      date: new Date(TEST_VTIME + 8000),
      finger: "XCVBN",
      inter: { months: 60, days: 1, milliseconds: 99 },
      stock: [
        { quantity: 42n, warehouse: "f" },
        { quantity: 21n, warehouse: "x" }
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
    reader = await parquet.ParquetReader.openFile('fruits-statistics.parquet');
    row = reader.metadata.row_groups[0];
  });

  it('column statistics should match input', async function() {
    
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

    assert.deepEqual(rowStats('stock,quantity').min_value, 10n);
    assert.deepEqual(rowStats('stock,quantity').max_value, 50n);
    assert.equal(+rowStats('stock,quantity').distinct_count, 9n);
    assert.equal(+rowStats('stock,quantity').null_count, 1n);

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

  it('columnIndex statistics should match input', async function() {

    /*  we split the data into pages by 3, so we should have page 1 with 3 recs and page 2 with 1 */

    const name = await reader.envelopeReader.readColumnIndex('name', row);
    assert.deepEqual(name.min_values, ['apples','banana']);
    assert.deepEqual(name.max_values, ['oranges','banana']);

    const quantity = await reader.envelopeReader.readColumnIndex('quantity', row);
    assert.deepEqual(quantity.min_values, [10n, undefined]);
    assert.deepEqual(quantity.max_values, [20n, undefined]);

    const price = await reader.envelopeReader.readColumnIndex('price', row);
    assert.deepEqual(price.min_values, [2.6, 3.2]);
    assert.deepEqual(price.max_values, [4.2, 3.2]);

    const day = await reader.envelopeReader.readColumnIndex('day', row);
    assert.deepEqual(day.min_values, [ new Date('2008-11-26'), new Date('2017-11-26') ]);
    assert.deepEqual(day.max_values, [ new Date('2018-03-03'), new Date('2017-11-26') ]);

    const finger = await reader.envelopeReader.readColumnIndex('finger', row);
    assert.deepEqual(finger.min_values, [ 'ABCDE', 'FNORD' ]);
    assert.deepEqual(finger.max_values, [ 'XCVBN', 'FNORD' ]);

    const stockQuantity = await reader.envelopeReader.readColumnIndex('stock,quantity', row);
    assert.deepEqual(stockQuantity.min_values, [ 10n, undefined ]);
    assert.deepEqual(stockQuantity.max_values, [ 50n, undefined ]);

    const stockWarehouse = await reader.envelopeReader.readColumnIndex('stock,warehouse', row);
    assert.deepEqual(stockWarehouse.min_values, [ 'A', undefined ]);
    assert.deepEqual(stockWarehouse.max_values, [ 'x', undefined ]);

    const colour = await reader.envelopeReader.readColumnIndex('colour', row);
    assert.deepEqual(colour.min_values, [ 'brown', 'yellow' ]);
    assert.deepEqual(colour.max_values, [ 'yellow', 'yellow' ]);
    
    const inter = await reader.envelopeReader.readColumnIndex('inter', row).catch(e => e);
    assert.equal(inter.message,'Column Index Missing');

    const meta_json = await reader.envelopeReader.readColumnIndex('meta_json', row).catch(e => e);
    assert.equal(meta_json.message,'Column Index Missing');
  });

  it('Setting pageIndex: false results in no column_index and no offset_index', async function() {
    let writer = await parquet.ParquetWriter.openFile(schema, 'fruits-no-index.parquet', {pageSize: 3, pageIndex: false});
    writer.appendRow({
      name: 'apples',
      quantity: 10n,
      price: 2.6,
      day: new Date('2017-11-26'),
      date: new Date(TEST_VTIME + 1000),
      finger: "FNORD",
      inter: { months: 10, days: 5, milliseconds: 777 },
      stock: [
        { quantity: 10n, warehouse: "A" },
        { quantity: 20n, warehouse: "B" }
      ],
      colour: [ 'green', 'red' ],
      meta_json: { expected_ship_date: TEST_VTIME }
    });
    await writer.close();

    let reader2 = await parquet.ParquetReader.openFile('fruits-no-index.parquet');
    reader2.metadata.row_groups[0].columns.forEach(column => {
      assert.equal(column.offset_index_offset, null);
      assert.equal(column.offset_index_length, null);
      assert.equal(column.column_index_offset, null);
      assert.equal(column.column_index_length, null);
    });
  });
});