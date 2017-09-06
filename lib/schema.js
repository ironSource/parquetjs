'use strict';
const parquet_thrift = require('../gen-nodejs/parquet_types')
const parquet_encodings = require('./encodings/supported');

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
    let opts = schema[name];

    /* field type */
    if (!opts.type || !opts.type in parquet_thrift.Type) {
      throw 'invalid parquet type: ' + col.opts;
    }

    /* field repetition type */
    let required = !opts.optional;
    let repeated = !!opts.repeated;
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
      type: opts.type,
      path: [name],
      repetition_type: repetition_type,
      encoding: opts.encoding,
      rLevelMax: rLevelMax,
      rLevelMax: dLevelMax
    });
  }

  return columns;
}

module.exports = { ParquetSchema };

