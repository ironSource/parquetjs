'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('../parquet.js');

const TEST_NUM_ROWS = 10000;

// FIXME: tempdir?
// write a new file 'fruits.parquet'
async function writeTestFile(opts) {
  let schema = new parquet.ParquetSchema({
    name:       { type: 'UTF8' },
    quantity:   { type: 'INT64', optional: true },
    price:      { type: 'DOUBLE' },
    date:       { type: 'TIMESTAMP_MICROS' },
    in_stock:   { type: 'BOOLEAN' },
    colour:     { type: 'UTF8', repeated: true, compression: opts.compression },
    meta_json:  { type: 'BSON', optional: true  },
  });

  let writer = await parquet.ParquetWriter.openFile(schema, 'fruits.parquet', opts);
  writer.setMetadata("myuid", "420");
  writer.setMetadata("fnord", "dronf");

  for (let i = 0; i < TEST_NUM_ROWS; ++i) {
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
      meta_json: { shape: 'curved' }
    });
  }

  await writer.close();
}

async function readTestFile() {
  let reader = await parquet.ParquetReader.openFile('fruits.parquet');
  assert.equal(reader.getRowCount(), TEST_NUM_ROWS * 4);
  assert.deepEqual(reader.getMetadata(), { "myuid": "420", "fnord": "dronf" })

  let schema = reader.getSchema();
  assert.equal(schema.columns.length, 7);
  assert.equal(schema.schema['name'].type, 'UTF8');
  assert.equal(schema.schema['name'].optional, false);
  assert.equal(schema.schema['name'].repeated, false);
  assert.equal(schema.schema['quantity'].type, 'INT64');
  assert.equal(schema.schema['quantity'].optional, true);
  assert.equal(schema.schema['quantity'].repeated, false);
  assert.equal(schema.schema['price'].type, 'DOUBLE');
  assert.equal(schema.schema['price'].optional, false);
  assert.equal(schema.schema['price'].repeated, false);
  assert.equal(schema.schema['date'].type, 'TIMESTAMP_MICROS');
  assert.equal(schema.schema['date'].optional, false);
  assert.equal(schema.schema['date'].repeated, false);
  assert.equal(schema.schema['in_stock'].type, 'BOOLEAN');
  assert.equal(schema.schema['in_stock'].optional, false);
  assert.equal(schema.schema['in_stock'].repeated, false);
  assert.equal(schema.schema['colour'].type, 'UTF8');
  assert.equal(schema.schema['colour'].optional, false);
  assert.equal(schema.schema['colour'].repeated, true);
  assert.equal(schema.schema['meta_json'].type, 'BSON');
  assert.equal(schema.schema['meta_json'].optional, true);
  assert.equal(schema.schema['meta_json'].repeated, false);

  {
    let cursor = reader.getCursor(['name']);
    for (let i = 0; i < TEST_NUM_ROWS; ++i) {
      assert.deepEqual(await cursor.next(), { name: 'apples' });
      assert.deepEqual(await cursor.next(), { name: 'oranges' });
      assert.deepEqual(await cursor.next(), { name: 'kiwi' });
      assert.deepEqual(await cursor.next(), { name: 'banana' });
    }

    assert.equal(await cursor.next(), null);
  }

  //{
  //  let cursor = reader.getCursor(['name', 'quantity']);
  //  for (let i = 0; i < 10000; ++i) {
  //    assert.deepEqual(await cursor.next(), { name: 'apples', quantity: 10 });
  //    assert.deepEqual(await cursor.next(), { name: 'oranges', quantity: 20 });
  //    assert.deepEqual(await cursor.next(), { name: 'kiwi' });
  //    assert.deepEqual(await cursor.next(), { name: 'banana' });
  //  }

  //  assert.equal(await cursor.next(), null);
  //}

  reader.close();
}

describe('Parquet', function() {
  this.timeout(60000);

  describe('with DataPageHeaderV1', function() {
    it('write a test file', function() {
      const opts = { useDataPageV2: false, compression: 'UNCOMPRESSED' };
      return writeTestFile(opts);
    });

    it('write a test file and then read it back', function() {
      const opts = { useDataPageV2: false, compression: 'UNCOMPRESSED' };
      return writeTestFile(opts).then(readTestFile);
    });
  });

  describe('with DataPageHeaderV2', function() {
    it('write a test file', function() {
      const opts = { useDataPageV2: true, compression: 'UNCOMPRESSED' };
      return writeTestFile(opts);
    });

    it('write a test file with GZIP compression', function() {
      const opts = { useDataPageV2: true, compression: 'GZIP' };
      return writeTestFile(opts);
    });

    it('write a test file with SNAPPY compression', function() {
      const opts = { useDataPageV2: true, compression: 'SNAPPY' };
      return writeTestFile(opts);
    });

    it('write a test file with LZO compression', function() {
      const opts = { useDataPageV2: true, compression: 'LZO' };
      return writeTestFile(opts);
    });

    it('write a test file with BROTLI compression', function() {
      const opts = { useDataPageV2: true, compression: 'BROTLI' };
      return writeTestFile(opts);
    });
  });

});
