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
    assert(schema.column_map.name);
    assert(schema.column_map.quantity);
    assert(schema.column_map.price);

    {
      const c = schema.column_map.name;
      assert.equal(c.name, 'name');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['name']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }

    {
      const c = schema.column_map.quantity;
      assert.equal(c.name, 'quantity');
      assert.equal(c.primitiveType, 'INT64');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['quantity']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }

    {
      const c = schema.column_map.price;
      assert.equal(c.name, 'price');
      assert.equal(c.primitiveType, 'DOUBLE');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['price']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }

  });

  it('should assign correct defaults in a flat schema with optional columns', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64', optional: true },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.columns.length, 3);
    assert(schema.column_map.name);
    assert(schema.column_map.quantity);
    assert(schema.column_map.price);

    {
      const c = schema.column_map.name;
      assert.equal(c.name, 'name');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['name']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }

    {
      const c = schema.column_map.quantity;
      assert.equal(c.name, 'quantity');
      assert.equal(c.primitiveType, 'INT64');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['quantity']);
      assert.equal(c.repetitionType, 'OPTIONAL');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 1);
    }

    {
      const c = schema.column_map.price;
      assert.equal(c.name, 'price');
      assert.equal(c.primitiveType, 'DOUBLE');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['price']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }
  });

  it('should assign correct defaults in a flat schema with repeated columns', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64', repeated: true },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.columns.length, 3);
    assert(schema.column_map.name);
    assert(schema.column_map.quantity);
    assert(schema.column_map.price);

    {
      const c = schema.column_map.name;
      assert.equal(c.name, 'name');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['name']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }

    {
      const c = schema.column_map.quantity;
      assert.equal(c.name, 'quantity');
      assert.equal(c.primitiveType, 'INT64');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['quantity']);
      assert.equal(c.repetitionType, 'REPEATED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 1);
      assert.equal(c.dLevelMax, 1);
    }

    {
      const c = schema.column_map.price;
      assert.equal(c.name, 'price');
      assert.equal(c.primitiveType, 'DOUBLE');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['price']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }
  });

  it('should assign correct defaults in a nested schema without repetition modifiers', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      stock: {
        fields: {
          quantity: { type: 'INT64' },
          warehouse: { type: 'UTF8' },
        }
      },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.columns.length, 4);
    assert(schema.column_map.name);
    assert(schema.column_map.stock);
    assert(schema.column_map.stock.fields.quantity);
    assert(schema.column_map.stock.fields.warehouse);
    assert(schema.column_map.price);

    {
      const c = schema.column_map.name;
      assert.equal(c.name, 'name');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['name']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }

    {
      const c = schema.column_map.stock;
      assert.equal(c.name, 'stock');
      assert.equal(c.primitiveType, undefined);
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['stock']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, undefined);
      assert.equal(c.compression, undefined);
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }

    {
      const c = schema.column_map.stock.fields.quantity;
      assert.equal(c.name, 'quantity');
      assert.equal(c.primitiveType, 'INT64');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['stock', 'quantity']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }

    {
      const c = schema.column_map.stock.fields.warehouse;
      assert.equal(c.name, 'warehouse');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['stock', 'warehouse']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }

    {
      const c = schema.column_map.price;
      assert.equal(c.name, 'price');
      assert.equal(c.primitiveType, 'DOUBLE');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['price']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }
  });

  it('should assign correct defaults in a nested schema with optional fields', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      stock: {
        optional: true,
        fields: {
          quantity: { type: 'INT64', optional: true },
          warehouse: { type: 'UTF8' },
        }
      },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.columns.length, 4);
    assert(schema.column_map.name);
    assert(schema.column_map.stock);
    assert(schema.column_map.stock.fields.quantity);
    assert(schema.column_map.stock.fields.warehouse);
    assert(schema.column_map.price);

    {
      const c = schema.column_map.name;
      assert.equal(c.name, 'name');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['name']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }

    {
      const c = schema.column_map.stock;
      assert.equal(c.name, 'stock');
      assert.equal(c.primitiveType, undefined);
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['stock']);
      assert.equal(c.repetitionType, 'OPTIONAL');
      assert.equal(c.encoding, undefined);
      assert.equal(c.compression, undefined);
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 1);
    }

    {
      const c = schema.column_map.stock.fields.quantity;
      assert.equal(c.name, 'quantity');
      assert.equal(c.primitiveType, 'INT64');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['stock', 'quantity']);
      assert.equal(c.repetitionType, 'OPTIONAL');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 2);
    }

    {
      const c = schema.column_map.stock.fields.warehouse;
      assert.equal(c.name, 'warehouse');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['stock', 'warehouse']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 1);
    }

    {
      const c = schema.column_map.price;
      assert.equal(c.name, 'price');
      assert.equal(c.primitiveType, 'DOUBLE');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['price']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }
  });

  it('should assign correct defaults in a nested schema with repeated fields', function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      stock: {
        repeated: true,
        fields: {
          quantity: { type: 'INT64', optional: true },
          warehouse: { type: 'UTF8' },
        }
      },
      price: { type: 'DOUBLE' },
    });

    assert.equal(schema.columns.length, 4);
    assert(schema.column_map.name);
    assert(schema.column_map.stock);
    assert(schema.column_map.stock.fields.quantity);
    assert(schema.column_map.stock.fields.warehouse);
    assert(schema.column_map.price);

    {
      const c = schema.column_map.name;
      assert.equal(c.name, 'name');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['name']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }

    {
      const c = schema.column_map.stock;
      assert.equal(c.name, 'stock');
      assert.equal(c.primitiveType, undefined);
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['stock']);
      assert.equal(c.repetitionType, 'REPEATED');
      assert.equal(c.encoding, undefined);
      assert.equal(c.compression, undefined);
      assert.equal(c.rLevelMax, 1);
      assert.equal(c.dLevelMax, 1);
    }

    {
      const c = schema.column_map.stock.fields.quantity;
      assert.equal(c.name, 'quantity');
      assert.equal(c.primitiveType, 'INT64');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['stock', 'quantity']);
      assert.equal(c.repetitionType, 'OPTIONAL');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 1);
      assert.equal(c.dLevelMax, 2);
    }

    {
      const c = schema.column_map.stock.fields.warehouse;
      assert.equal(c.name, 'warehouse');
      assert.equal(c.primitiveType, 'BYTE_ARRAY');
      assert.equal(c.originalType, 'UTF8');
      assert.deepEqual(c.path, ['stock', 'warehouse']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 1);
      assert.equal(c.dLevelMax, 1);
    }

    {
      const c = schema.column_map.price;
      assert.equal(c.name, 'price');
      assert.equal(c.primitiveType, 'DOUBLE');
      assert.equal(c.originalType, undefined);
      assert.deepEqual(c.path, ['price']);
      assert.equal(c.repetitionType, 'REQUIRED');
      assert.equal(c.encoding, 'PLAIN');
      assert.equal(c.compression, 'UNCOMPRESSED');
      assert.equal(c.rLevelMax, 0);
      assert.equal(c.dLevelMax, 0);
    }
  });

});
