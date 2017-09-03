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

function buildColumns(schema, rlvl_max_parent, dlvl_max_parent) {
  if (!rlvl_max_parent) {
    rlvl_max_parent = 0;
  }

  if (!dlvl_max_parent) {
    dlvl_max_parent = 0;
  }

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
    let rlvl_max = rlvl_max_parent;
    let dlvl_max = dlvl_max_parent;

    let repetition_type = "REQUIRED";
    if (!required) {
      repetition_type = "OPTIONAL";
      ++dlvl_max;
    }

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
      rlevel_max: rlvl_max,
      dlevel_max: dlvl_max
    });
  }

  return columns;
}

module.exports = { ParquetSchema };

