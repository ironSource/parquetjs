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
    this.columns.push(col);
  }

};

module.exports = { ParquetSchema };

