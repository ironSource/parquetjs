"use strict";
var parquet_thrift = require('../gen-nodejs/parquet_types')
var parquet_encodings = require('./encodings/supported');

/**
 * A parquet file schema
 */
class ParquetSchema {

  constructor(schema) {
    this.schema = schema;
    this.columns = buildColumns(schema);
  }

};

function buildColumns(schema) {
  let columns = [];

  for (let name in schema) {
    let opts = schema[name];

    /* field type */
    if (!opts.type || !opts.type in parquet_thrift.Type) {
      throw "invalid parquet type: " + col.opts;
    }

    /* field repetition type */
    let required = !opts.optional;
    let repeated = !!opts.repeated;

    let repetition_type = required ? "REQUIRED" : "OPTIONAL";
    if (repeated) {
      repetition_type = "REPEATED";
    }

    /* field encoding */
    if (!opts.encoding) {
      opts.encoding = "PLAIN";
    }

    if (parquet_encodings.SUPPORTED_ENCODINGS.indexOf(opts.encoding) < 0) {
      throw "unsupported parquet encoding: " + opts.encodig;
    }

    /* add to schema */
    columns.push({
      name: name,
      type: opts.type,
      path: [name],
      repetition_type: repetition_type,
      encoding: opts.encoding,
      rlevel_max: 0,
      dlevel_max: 0,
    });
  }

  return columns;
}

module.exports = { ParquetSchema };

