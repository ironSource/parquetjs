"use strict";
var thrift = require('thrift');
var parquet_thrift = require('../gen-nodejs/parquet_types')
var parquet_shredder = require('./shred')

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = "PAR1";

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 1;

/**
 * Write a parquet file to an abstract output stream without buffering
 */
function ParquetWriter(schema, output_stream) {
  var os = output_stream;
  var os_offset = 0;
  var row_count = 0;
  var row_groups = [];

  function write(buf) {
    os_offset += buf.length;
    os.write(buf);
  }

  this.writeHeader = function() {
    write(Buffer.from(PARQUET_MAGIC));
  }

  this.writeRowGroup = function(rows) {
    var rows_shredded = parquet_shredder.shredRecords(schema, rows);

    var rgroup = new parquet_thrift.RowGroup();
    rgroup.num_rows = rows.length;
    rgroup.columns = [];
    rgroup.total_byte_size = 0;

    schema.columns.forEach(function(col_def) {
      var cchunk_data = encodeColumnChunk(
          col_def,
          rows_shredded[col_def.name],
          os_offset);

      var cchunk = new parquet_thrift.ColumnChunk();
      cchunk.file_offset = cchunk_data.metadata_offset;
      cchunk.meta_data = cchunk_data.metadata;
      console.log(cchunk);
      rgroup.columns.push(cchunk);
      rgroup.total_byte_size += cchunk_data.body.length;

      write(cchunk_data.body);
    });

    row_count += rows.length;
    row_groups.push(rgroup);
  }

  this.writeFooter = function() {
    if (row_count == 0) {
      throw "can't write parquet file with zero rows";
    }

    if (schema.columns.length == 0) {
      throw "can't write parquet file with zero columns";
    }

    // prepare parquet file metadata message
    var fmd = new parquet_thrift.FileMetaData()
    fmd.version = PARQUET_VERSION;
    fmd.created_by = "parquet.js";
    fmd.num_rows = row_count;
    fmd.row_groups = row_groups;
    fmd.schema = [];

    schema.columns.forEach(function(col_def) {
      var tcol = new parquet_thrift.SchemaElement();
      tcol.name = col_def.name;
      tcol.type = col_def.thrift_type;
      fmd.schema.push(tcol);
    });

    // write footer data
    var fmd_bin = serializeThrift(fmd);
    write(fmd_bin)

    // 4 bytes footer length, uint32 LE
    var footer = new Buffer(8);
    footer.writeUInt32LE(fmd_bin.length);

    // 4 bytes footer magic 'PAR1'
    footer.fill(PARQUET_MAGIC, 4);
    write(footer);
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
    this.writer = new ParquetWriter(schema, this.os);
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
 * Encode an array of values into a parquet column chunk
 */
function encodeColumnChunk(column_definition, values, base_offset) {
  var pages = [];

  var pages_buf = Buffer.concat(pages);

  var metadata = new parquet_thrift.ColumnMetaData();
  metadata.type = column_definition.thrift_type;
  metadata.path_in_schema = [column_definition.name];
  metadata.num_values = values.length;

  var metadata_offset = base_offset + pages_buf.length;

  var body = Buffer.concat([pages_buf, serializeThrift(metadata)]);

  return { body, metadata, metadata_offset };
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
};

