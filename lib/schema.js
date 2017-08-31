"use strict";
var parquet_thrift = require('../gen-nodejs/parquet_types')

/**
 * A parquet file schema
 */
class ParquetSchema {

  constructor() {
    this.columns = [];
  }

  /**
   * Adds a new column to the schema -- nested columns are currently not supported
   */
  addColumn(col) {
    if (!col.type || !col.type in parquet_thrift.Type) {
      throw "invalid parquet type: " + col.type;
    }

    col.path = [col.name];
    col.repetition_type = "REQUIRED"; // FIXME
    this.columns.push(col);
  }

};

module.exports = { ParquetSchema };

