'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('../parquet.js');

describe('ParquetShredder', function() {

  it('should shred a single simple record', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64' },
      price: { type: 'DOUBLE' },
    });


    let buf = {};

    {
      let rec = { name: "apple", quantity: 10, price: 23.5 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    let colData = buf.columnData;
    assert.equal(buf.rowCount, 1);
    assert.deepEqual(colData.name.dlevels, [0]);
    assert.deepEqual(colData.name.rlevels, [0]);
    assert.deepEqual(colData.name.values.map((x) => x.toString()), ["apple"]);
    assert.deepEqual(colData.quantity.dlevels, [0]);
    assert.deepEqual(colData.quantity.rlevels, [0]);
    assert.deepEqual(colData.quantity.values, [10]);
    assert.deepEqual(colData.price.dlevels, [0]);
    assert.deepEqual(colData.price.rlevels, [0]);
    assert.deepEqual(colData.price.values, [23.5]);
  });

  it('should shred a list of simple records', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64' },
      price: { type: 'DOUBLE' },
    });


    let buf = {};

    {
      let rec = { name: "apple", quantity: 10, price: 23.5 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "orange", quantity: 20, price: 17.1 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "banana", quantity: 15, price: 42 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    let colData = buf.columnData;
    assert.equal(buf.rowCount, 3);
    assert.deepEqual(colData.name.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.name.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.name.values.map((x) => x.toString()), ["apple", "orange", "banana"]);
    assert.deepEqual(colData.quantity.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.quantity.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.quantity.values, [10, 20, 15]);
    assert.deepEqual(colData.price.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.price.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.price.values, [23.5, 17.1, 42]);
  });

  it('should shred a list of simple records with optional scalar fields', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64', optional: true },
      price: { type: 'DOUBLE' },
    });


    let buf = {};

    {
      let rec = { name: "apple", quantity: 10, price: 23.5 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "orange", price: 17.1 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "banana", quantity: 15, price: 42 };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    let colData = buf.columnData;
    assert.equal(buf.rowCount, 3);
    assert.deepEqual(colData.name.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.name.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.name.values.map((x) => x.toString()), ["apple", "orange", "banana"]);
    assert.deepEqual(colData.quantity.dlevels, [1, 0, 1]);
    assert.deepEqual(colData.quantity.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.quantity.values, [10, 15]);
    assert.deepEqual(colData.price.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.price.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.price.values, [23.5, 17.1, 42]);
  });

  it('should shred a list of simple records with repeated scalar fields', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      colours: { type: 'UTF8', repeated: true },
      price: { type: 'DOUBLE' },
    });


    let buf = {};

    {
      let rec = { name: "apple", price: 23.5, colours: ["red", "green"] };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "orange", price: 17.1, colours: ["orange"] };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    {
      let rec = { name: "banana", price: 42, colours: ["yellow"] };
      parquet.ParquetShredder.shredRecord(schema, rec, buf);
    }

    let colData = buf.columnData;
    assert.equal(buf.rowCount, 3);
    assert.deepEqual(colData.name.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.name.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.name.values.map((x) => x.toString()), ["apple", "orange", "banana"]);
    assert.deepEqual(colData.name.count, 3);
    assert.deepEqual(colData.colours.dlevels, [1, 1, 1, 1]);
    assert.deepEqual(colData.colours.rlevels, [0, 1, 0, 0]);
    assert.deepEqual(colData.colours.values.map((x) => x.toString()), ["red", "green", "orange", "yellow"]);
    assert.deepEqual(colData.colours.count, 4);
    assert.deepEqual(colData.price.dlevels, [0, 0, 0]);
    assert.deepEqual(colData.price.rlevels, [0, 0, 0]);
    assert.deepEqual(colData.price.values, [23.5, 17.1, 42]);
    assert.deepEqual(colData.price.count, 3);
  });

});
