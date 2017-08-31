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
 * Writes the envelope/outer structure of a parquet file to an output stream
 */
class ParquetEnvelopeWriter {

  constructor(write_cb) {
    this.write_cb = write_cb;
    this.write_cb(Buffer.from(PARQUET_MAGIC));
  }

  writeRowGroup(row_group) {
    this.write_cb(row_group)
  }

  writeFooter(footer) {
    // footer data
    this.write_cb(footer)

    // 4 bytes footer length, uint32 LE
    var buf = new Buffer(8);
    buf.writeUInt32LE(footer.length);

    // 4 bytes footer magic 'PAR1'
    buf.fill(PARQUET_MAGIC, 4);
    this.write_cb(buf);
  }

}

/**
 * Write a parquet file to an abstract output stream
 */
function ParquetWriter(schema, write_cb) {
  var envelope_writer = new ParquetEnvelopeWriter(write_cb);
  var row_count = 0;
  var row_groups = [];

  this.end = function() {
    if (row_count == 0) {
      throw "can't write parquet file with zero rows";
    }

    if (schema.elements.length == 0) {
      throw "can't write parquet file with zero columns";
    }

    var fmd = new parquet_thrift.FileMetaData()
    fmd.version = PARQUET_VERSION;
    fmd.created_by = "parquet.js";
    fmd.num_rows = row_count;
    fmd.row_groups = row_groups;
    fmd.schema = schema.elements;

    envelope_writer.writeFooter(serializeThrift(fmd));
  }

}

/**
 * Write a parquet file to the file system
 */
class ParquetFileWriter {

  constructor(schema, path) {
    this.os = fs.createWriteStream(path);

    var write_cb = (function(t) {
      return function(buffer) {
        t.os.write(buffer);
      };
    })(this);

    this.writer = new ParquetWriter(schema, write_cb);
  }

  end() {
    this.writer.end();
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

