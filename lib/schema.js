'use strict';
const parquet_codec = require('./codec');
const parquet_compression = require('./compression');
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
      this.column_map[colDef.path] = colDef;
    }
  }

  findColumn(path) {
    return this.column_map[getColumnKey(path)];
  }

};

function buildColumns(schema, rLevelParentMax, dLevelParentMax, path) {
  if (!rLevelParentMax) {
    rLevelParentMax = 0;
  }

  if (!dLevelParentMax) {
    dLevelParentMax = 0;
  }

  if (!path) {
    path = [];
  }

  let columns = [];
  for (let name in schema) {
    const opts = schema[name];

    /* field repetition type */
    const required = !opts.optional;
    const repeated = !!opts.repeated;
    let rLevelMax = rLevelParentMax;
    let dLevelMax = dLevelParentMax;

    let repetitionType = 'REQUIRED';
    if (!required) {
      repetitionType = 'OPTIONAL';
      ++dLevelMax;
    }

    if (repeated) {
      repetitionType = 'REPEATED';
      ++rLevelMax;

      if (required) {
        ++dLevelMax;
      }
    }

    /* nested field */
    if (opts.fields) {
      columns.push({
        name: name,
        path: path.concat([name]),
        repetitionType: repetitionType,
        rLevelMax: rLevelMax,
        dLevelMax: dLevelMax
      });

      columns = columns.concat(
          buildColumns(
              opts.fields,
              rLevelMax,
              dLevelMax,
              path.concat([name])));

      continue;
    }

    /* field type */
    const typeDef = parquet_types.PARQUET_LOGICAL_TYPES[opts.type];
    if (!typeDef) {
      throw 'invalid parquet type: ' + opts.type;
    }

    /* field encoding */
    if (!opts.encoding) {
      opts.encoding = 'PLAIN';
    }

    if (!(opts.encoding in parquet_codec)) {
      throw 'unsupported parquet encoding: ' + opts.encodig;
    }

    if (!opts.compression) {
      opts.compression = 'UNCOMPRESSED';
    }

    if (!(opts.compression in parquet_compression.PARQUET_COMPRESSION_METHODS)) {
      throw 'unsupported compression method: ' + opts.compression;
    }

    /* add to schema */
    columns.push({
      name: name,
      primitiveType: typeDef.primitiveType,
      originalType: typeDef.originalType,
      path: path.concat([name]),
      repetitionType: repetitionType,
      encoding: opts.encoding,
      compression: opts.compression,
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

