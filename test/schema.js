'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('../parquet.js');

describe('ParquetSchema', function() {

  it('should assign correct defaults in a simple flat schema', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64' },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.columns.length, 3);
    assert.deepEqual(schema.columns[0], schema.column_map["name"]);
    assert.deepEqual(schema.columns[1], schema.column_map["quantity"]);
    assert.deepEqual(schema.columns[2], schema.column_map["price"]);

    assert.equal(schema.columns[0].name,  'name');
    assert.equal(schema.columns[0].primitiveType,  'BYTE_ARRAY');
    assert.equal(schema.columns[0].originalType,  'UTF8');
    assert.deepEqual(schema.columns[0].path,  ['name']);
    assert.equal(schema.columns[0].repetitionType,  'REQUIRED');
    assert.equal(schema.columns[0].encoding,  'PLAIN');
    assert.equal(schema.columns[0].compression,  'UNCOMPRESSED');
    assert.equal(schema.columns[0].rLevelMax,  0);
    assert.equal(schema.columns[0].dLevelMax,  0);

    assert.equal(schema.columns[1].name,  'quantity');
    assert.equal(schema.columns[1].primitiveType,  'INT64');
    assert.equal(schema.columns[1].originalType,  undefined);
    assert.deepEqual(schema.columns[1].path,  ['quantity']);
    assert.equal(schema.columns[1].repetitionType,  'REQUIRED');
    assert.equal(schema.columns[1].encoding,  'PLAIN');
    assert.equal(schema.columns[1].compression,  'UNCOMPRESSED');
    assert.equal(schema.columns[1].rLevelMax,  0);
    assert.equal(schema.columns[1].dLevelMax,  0);

    assert.equal(schema.columns[2].name,  'price');
    assert.equal(schema.columns[2].primitiveType,  'DOUBLE');
    assert.equal(schema.columns[2].originalType,  undefined);
    assert.deepEqual(schema.columns[2].path,  ['price']);
    assert.equal(schema.columns[2].repetitionType,  'REQUIRED');
    assert.equal(schema.columns[2].encoding,  'PLAIN');
    assert.equal(schema.columns[2].compression,  'UNCOMPRESSED');
    assert.equal(schema.columns[2].rLevelMax,  0);
    assert.equal(schema.columns[2].dLevelMax,  0);
  });

  it('should assign correct defaults in a flat schema with optional columns', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64', optional: true },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.columns.length, 3);
    assert.deepEqual(schema.columns[0], schema.column_map["name"]);
    assert.deepEqual(schema.columns[1], schema.column_map["quantity"]);
    assert.deepEqual(schema.columns[2], schema.column_map["price"]);

    assert.equal(schema.columns[0].name,  'name');
    assert.equal(schema.columns[0].primitiveType,  'BYTE_ARRAY');
    assert.equal(schema.columns[0].originalType,  'UTF8');
    assert.deepEqual(schema.columns[0].path,  ['name']);
    assert.equal(schema.columns[0].repetitionType,  'REQUIRED');
    assert.equal(schema.columns[0].encoding,  'PLAIN');
    assert.equal(schema.columns[0].compression,  'UNCOMPRESSED');
    assert.equal(schema.columns[0].rLevelMax,  0);
    assert.equal(schema.columns[0].dLevelMax,  0);

    assert.equal(schema.columns[1].name,  'quantity');
    assert.equal(schema.columns[1].primitiveType,  'INT64');
    assert.equal(schema.columns[1].originalType,  undefined);
    assert.deepEqual(schema.columns[1].path,  ['quantity']);
    assert.equal(schema.columns[1].repetitionType,  'OPTIONAL');
    assert.equal(schema.columns[1].encoding,  'PLAIN');
    assert.equal(schema.columns[1].compression,  'UNCOMPRESSED');
    assert.equal(schema.columns[1].rLevelMax,  0);
    assert.equal(schema.columns[1].dLevelMax,  1);

    assert.equal(schema.columns[2].name,  'price');
    assert.equal(schema.columns[2].primitiveType,  'DOUBLE');
    assert.equal(schema.columns[2].originalType,  undefined);
    assert.deepEqual(schema.columns[2].path,  ['price']);
    assert.equal(schema.columns[2].repetitionType,  'REQUIRED');
    assert.equal(schema.columns[2].encoding,  'PLAIN');
    assert.equal(schema.columns[2].compression,  'UNCOMPRESSED');
    assert.equal(schema.columns[2].rLevelMax,  0);
    assert.equal(schema.columns[2].dLevelMax,  0);
  });

  it('should assign correct defaults in a flat schema with repeated columns', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64', repeated: true },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.columns.length, 3);
    assert.deepEqual(schema.columns[0], schema.column_map["name"]);
    assert.deepEqual(schema.columns[1], schema.column_map["quantity"]);
    assert.deepEqual(schema.columns[2], schema.column_map["price"]);

    assert.equal(schema.columns[0].name,  'name');
    assert.equal(schema.columns[0].primitiveType,  'BYTE_ARRAY');
    assert.equal(schema.columns[0].originalType,  'UTF8');
    assert.deepEqual(schema.columns[0].path,  ['name']);
    assert.equal(schema.columns[0].repetitionType,  'REQUIRED');
    assert.equal(schema.columns[0].encoding,  'PLAIN');
    assert.equal(schema.columns[0].compression,  'UNCOMPRESSED');
    assert.equal(schema.columns[0].rLevelMax,  0);
    assert.equal(schema.columns[0].dLevelMax,  0);

    assert.equal(schema.columns[1].name,  'quantity');
    assert.equal(schema.columns[1].primitiveType,  'INT64');
    assert.equal(schema.columns[1].originalType,  undefined);
    assert.deepEqual(schema.columns[1].path,  ['quantity']);
    assert.equal(schema.columns[1].repetitionType,  'REPEATED');
    assert.equal(schema.columns[1].encoding,  'PLAIN');
    assert.equal(schema.columns[1].compression,  'UNCOMPRESSED');
    assert.equal(schema.columns[1].rLevelMax,  1);
    assert.equal(schema.columns[1].dLevelMax,  1);

    assert.equal(schema.columns[2].name,  'price');
    assert.equal(schema.columns[2].primitiveType,  'DOUBLE');
    assert.equal(schema.columns[2].originalType,  undefined);
    assert.deepEqual(schema.columns[2].path,  ['price']);
    assert.equal(schema.columns[2].repetitionType,  'REQUIRED');
    assert.equal(schema.columns[2].encoding,  'PLAIN');
    assert.equal(schema.columns[2].compression,  'UNCOMPRESSED');
    assert.equal(schema.columns[2].rLevelMax,  0);
    assert.equal(schema.columns[2].dLevelMax,  0);
  });

});
