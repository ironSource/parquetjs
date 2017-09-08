'use strict';
const parquet_codec = require('./codec');
const parquet_types = require('./types');
const parquet_util = require('./util');

const PARQUET_COLUMN_KEY_SEPARATOR = '.';

/**
 * A parquet file schema
 */
class ParquetSchema {

  /**
   * Load a parquet file schma from a parquet file metadata structure
   */
  static fromMetadata(metadata) {
    return new ParquetSchema(buildSchemaFromMetadata(metadata));
  }

  /**
   * Create a new schema from a JSON schema definition
   */
  constructor(schema) {
    this.schema = schema;
    this.columns = buildColumns(schema);
    this.column_map = {};

    for (let colDef of this.columns) {
      this.column_map[getColumnKey([colDef.name])] = colDef;
    }
  }

  findColumn(path) {
    return this.column_map[getColumnKey(path)];
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
    const typeDef = parquet_types.PARQUET_LOGICAL_TYPES[opts.type];
    if (!typeDef) {
      throw 'invalid parquet type: ' + opts.type;
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

    if (!(opts.encoding in parquet_codec)) {
      throw 'unsupported parquet encoding: ' + opts.encodig;
    }

    /* add to schema */
    columns.push({
      name: name,
      primitiveType: typeDef.primitiveType,
      originalType: typeDef.originalType,
      path: [name],
      repetition_type: repetition_type,
      encoding: opts.encoding,
      rLevelMax: rLevelMax,
      dLevelMax: dLevelMax
    });
  }

  return columns;
}

function getColumnKey(path) {
  return path.join(PARQUET_COLUMN_KEY_SEPARATOR);
}

module.exports = { ParquetSchema, getColumnKey };

