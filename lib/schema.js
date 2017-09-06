'use strict';
const parquet_thrift = require('../gen-nodejs/parquet_types')
const parquet_encodings = require('./encodings/supported');

const PARQUET_LOGICAL_TYPES = {
  'BOOLEAN': {
    type: 'BOOLEAN'
  },
  'INT32': {
    type: 'INT32'
  },
  'INT64': {
    type: 'INT64'
  },
  'INT96': {
    type: 'INT96'
  },
  'FLOAT': {
    type: 'FLOAT'
  },
  'DOUBLE': {
    type: 'DOUBLE'
  },
  'BYTE_ARRAY': {
    type: 'BYTE_ARRAY'
  }
};

/**
 * A parquet file schema
 */
class ParquetSchema {

  constructor(schema) {
    this.schema = schema;
    this.columns = buildColumns(schema);
  }

};

function buildColumns(schema, rLevelParentMax, dLevelParentMax) {
  if (!rLevelParentMax) {
    rLevelParentMax = 0;
  }

  if (!dLevelParentMax) {
    dLevelParentMax = 0;
  }

  let columns = [];
  for (let name in schema) {
    const opts = schema[name];

    /* field type */
    const typeDef = PARQUET_LOGICAL_TYPES[opts.type];
    if (!typeDef) {
      throw 'invalid parquet type: ' + col.opts.type;
    }

    /* field repetition type */
    const required = !opts.optional;
    const repeated = !!opts.repeated;
    let rLevelMax = rLevelParentMax;
    let dLevelMax = dLevelParentMax;

    let repetition_type = 'REQUIRED';
    if (!required) {
      repetition_type = 'OPTIONAL';
      ++dLevelMax;
    }

    if (repeated) {
      repetition_type = 'REPEATED';
      ++rLevelMax;

      if (required) {
        ++dLevelMax;
      }
    }

    /* field encoding */
    if (!opts.encoding) {
      opts.encoding = 'PLAIN';
    }

    if (parquet_encodings.SUPPORTED_ENCODINGS.indexOf(opts.encoding) < 0) {
      throw 'unsupported parquet encoding: ' + opts.encodig;
    }

    /* add to schema */
    columns.push({
      name: name,
      type: typeDef.type,
      path: [name],
      repetition_type: repetition_type,
      encoding: opts.encoding,
      rLevelMax: rLevelMax,
      dLevelMax: dLevelMax
    });
  }

  return columns;
}

module.exports = { ParquetSchema };

