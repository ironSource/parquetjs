"use strict";
var thrift = require('thrift');
var parquet_thrift = require('../gen-nodejs/parquet_types')

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = "PAR1";

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 1;


/**
 * A parquet file schema
 */
class ParquetSchema {

  constructor() {
    this.elements = [];
  }

  /**
   * Adds a new column to the schema -- nested columns are currently not supported
   */
  addColumn(name, type, opts) {
    var tcol = new parquet_thrift.SchemaElement();
    tcol.name = name;

    this.elements.push(tcol);
  }

};

/**
 * Write a parquet file to an abstract output stream without buffering
 */
function ParquetWriter(schema, write_cb) {
  var row_count = 0;
  var row_groups = [];

  this.writeHeader = function() {
    write_cb(Buffer.from(PARQUET_MAGIC));
  }

  this.writeRowGroup = function(rows) {
    console.log("write row group", rows);
    row_count += rows.length;
  }

  this.writeFooter = function() {
    if (row_count == 0) {
      throw "can't write parquet file with zero rows";
    }

    if (schema.elements.length == 0) {
      throw "can't write parquet file with zero columns";
    }

    // prepare parquet file metadata message
    var fmd = new parquet_thrift.FileMetaData()
    fmd.version = PARQUET_VERSION;
    fmd.created_by = "parquet.js";
    fmd.num_rows = row_count;
    fmd.row_groups = row_groups;
    fmd.schema = schema.elements;

    // write footer data
    var fmd_bin = serializeThrift(fmd);
    write_cb(fmd_bin)

    // 4 bytes footer length, uint32 LE
    var footer = new Buffer(8);
    footer.writeUInt32LE(fmd_bin.length);

    // 4 bytes footer magic 'PAR1'
    footer.fill(PARQUET_MAGIC, 4);
    write_cb(footer);
  };

}

/**
 * Write a parquet file to the file system. The ParquetFileWriter will perform
 * buffering/batching for performance, so end() must be called after all rows
 * are written
 */
class ParquetFileWriter {

  constructor(schema, path) {
    this.os = fs.createWriteStream(path);
    this.row_buffer = [];
    this.row_buffer_maxlen = 8; // FIXME

    var write_cb = (function(t) {
      return function(buffer) {
        t.os.write(buffer);
      };
    })(this);

    this.writer = new ParquetWriter(schema, write_cb);
    this.writer.writeHeader();
  }

  writeRow(row) {
    this.row_buffer.push(row);

    if (this.row_buffer.length >= this.row_buffer_maxlen) {
      this.writer.writeRowGroup(this.row_buffer);
      this.row_buffer = [];
    }
  }

  end() {
    if (this.row_buffer.length > 0) {
      this.writer.writeRowGroup(this.row_buffer);
      this.row_buffer = [];
    }

    this.writer.writeFooter();
    this.os.end();
    this.os = null;
  }

}

/**
 * Helper function that serializes a thrift object into a buffer
 */
function serializeThrift(obj) {
  var output = []

  var transport = new thrift.TBufferedTransport(null, function (buf) {
    output.push(buf)
  })

  var protocol = new thrift.TCompactProtocol(transport)
  obj.write(protocol)
  transport.flush()

  return Buffer.concat(output)
}

module.exports = {
  ParquetWriter,
  ParquetFileWriter,
  ParquetSchema
};

