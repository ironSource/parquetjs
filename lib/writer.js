"use strict";
var fs = require('fs');
var thrift = require('thrift');
var parquet_thrift = require('../gen-nodejs/parquet_types')
var parquet_shredder = require('./shred')
var parquet_util = require('./util')
var codec_plain = require('./encodings/plain')
var codec_rle = require('./encodings/rle')

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = "PAR1";

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 1;

/**
 * Default Page and Row Group sizes
 */
const DEFAULT_PAGE_SIZE = 8192;
const DEFAULT_ROW_GROUP_SIZE = 4096;
const DEFAULT_PAGE_COMPRESSION = "UNCOMPRESSED";

/**
 * Repetition and Definition Level Encoding
 */
const PARQUET_RDLVL_TYPE = "INT32";
const PARQUET_RDLVL_ENCODING = "RLE";

/**
 * Write a parquet file to the file system. The ParquetFileWriter will perform
 * buffering/batching for performance, so end() must be called after all rows
 * are written
 */
class ParquetFileWriter {

  /**
   * Create a new parquet file writer
   */
  constructor(schema, path) {
    this.os = fs.createWriteStream(path);
    this.row_buffer = [];
    this.row_group_size = DEFAULT_ROW_GROUP_SIZE;
    this.writer = new ParquetWriter(schema, this.os);
    this.writer.writeHeader();
  }

  /**
   * Append a single row to the parquet file. Rows are buffered in memory
   * until row_group_size rows are in the buffer or end() is called
   */
  appendRow(row) {
    if (!this.os) {
      throw "end() was called";
    }

    this.row_buffer.push(row);

    if (this.row_buffer.length >= this.row_group_size) {
      this.writer.writeRowGroup(this.row_buffer);
      this.row_buffer = [];
    }
  }

  /**
   * Finish writing the parquet file and commit the footer to disk. This method
   * MUST be called after you are finished adding rows. You must not call this
   * method twice on the same object or add any rows after the end() method was called
   */
  end() {
    if (!this.os) {
      throw "end() was called";
    }

    if (this.row_buffer.length > 0) {
      this.writer.writeRowGroup(this.row_buffer);
      this.row_buffer = [];
    }

    this.writer.writeFooter();
    this.os.end();
    this.os = null;
  }

  /**
   * Set the parquet row group size. This values controls the maximum number
   * of rows that are buffered in memory at any given time as well as the number
   * of rows that are co-located on disk. A higher value is generally better for
   * read-time I/O performance at the tradeoff of write-time memory usage.
   */
  setRowGroupSize(cnt) {
    this.row_group_size = cnt;
  }

  /**
   * Set the parquet data page size. The data page size controls the maximum 
   * number of column values that are written to disk as a consecutive array
   */
  setPageSize(cnt) {
    this.writer.setPageSize(cnt);
  }

}

/**
 * Write a parquet file to an abstract output stream without buffering. This
 * class is intendend for advanced and internal users. The writeXXX methods
 * must be called in the correct order to produce a valid file. The  output_stream
 * object must support a write(buffer) method.
 */
function ParquetWriter(schema, output_stream) {
  let os = output_stream;
  let os_offset = 0;
  let row_count = 0;
  let row_groups = [];
  let page_size = DEFAULT_PAGE_SIZE;

  function write(buf) {
    os_offset += buf.length;
    os.write(buf);
  }

  /**
   * Write the parquet file header
   */
  this.writeHeader = function() {
    write(Buffer.from(PARQUET_MAGIC));
  }

  /**
   * Write a parquet row group
   */
  this.writeRowGroup = function(rows) {
    let rows_shredded = parquet_shredder.shredRecords(schema, rows);
    let rgroup = encodeRowGroup(schema, rows.length, rows_shredded, {
      base_offset: os_offset,
      page_size: page_size
    });

    row_count += rows.length;
    row_groups.push(rgroup.metadata);
    write(rgroup.body);
  }

  /**
   * Write the parquet file footer
   */
  this.writeFooter = function() {
    if (row_count == 0) {
      throw "can't write parquet file with zero rows";
    }

    if (schema.columns.length == 0) {
      throw "can't write parquet file with zero columns";
    }

    write(encodeFooter(schema, row_count, row_groups));
  };

  /**
   * Set the parquet data page size. The data page size controls the maximum 
   * number of column values that are written to disk as a consecutive array
   */
  this.setPageSize = function(cnt) {
    page_size = cnt;
  }

}

/**
 * Encode a parquet data page
 */
function encodeDataPage(column, value_count, values, rlevels, dlevels) {
  /* encode values */
  let values_buf;
  switch (column.encoding) {

    case "PLAIN":
      values_buf = codec_plain.encodeValues(column.type, values);
      break;

    case "RLE":
      values_buf = codec_rle.encodeValues(column.type, values);
      break;

    default:
      throw "invalid encoding: " + encoding;

  }

  /* encode repetition and definition levels */
  let rlevels_buf = codec_rle.encodeValues(
      PARQUET_RDLVL_TYPE,
      rlevels,
      { bit_width: parquet_util.getBitWidth(column.rlevel_max) });

  let dlevels_buf = codec_rle.encodeValues(
      PARQUET_RDLVL_TYPE,
      dlevels,
      { bit_width: parquet_util.getBitWidth(column.dlevel_max) });

  /* build page header */
  let page_body = Buffer.concat([rlevels_buf, dlevels_buf, values_buf]);
  let page_header = new parquet_thrift.PageHeader()
  page_header.num_values = value_count;
  page_header.type = 
      parquet_util.setThriftEnum(parquet_thrift.PageType, "DATA_PAGE");

  page_header.encoding =
      parquet_util.setThriftEnum(parquet_thrift.Encoding, column.encoding);

  page_header.definition_level_encoding =
    parquet_util.setThriftEnum(parquet_thrift.Encoding, PARQUET_RDLVL_ENCODING);

  page_header.repetition_level_encoding =
      parquet_util.setThriftEnum(parquet_thrift.Encoding, PARQUET_RDLVL_ENCODING);

  page_header.uncompressed_page_size = page_body.length;
  page_header.compressed_page_size = page_body.length;
  page_header.data_page_header = new parquet_thrift.DataPageHeader();
  page_header.data_page_header.num_values = page_header.num_values;
  page_header.data_page_header.encoding = page_header.encoding;

  page_header.data_page_header.definition_level_encoding =
      page_header.definition_level_encoding;

  page_header.data_page_header.repetition_level_encoding =
      page_header.repetition_level_encoding;

  return Buffer.concat([parquet_util.serializeThrift(page_header), page_body]);
}

/**
 * Encode an array of values into a parquet column chunk
 */
function encodeColumnChunk(opts) {
  let metadata = new parquet_thrift.ColumnMetaData();
  metadata.type = parquet_util.setThriftEnum(parquet_thrift.Type, opts.column.type);
  metadata.path_in_schema = opts.column.path;
  metadata.num_values = opts.values.length;
  metadata.data_page_offset = opts.base_offset;
  metadata.encodings = [];

  /* list encodings */
  let encodings_set = {};
  encodings_set[PARQUET_RDLVL_ENCODING] = true;
  encodings_set[opts.column.encoding] = true;
  for (let k in encodings_set) {
    metadata.encodings.push(parquet_util.setThriftEnum(parquet_thrift.Encoding, k));
  }

  /* build pages */
  let values = opts.values;
  let pages = [];

  for (let voff = 0; voff <= values.length; voff += opts.page_size) {
    /* extract page data vectors */
    let page_values = [];
    let page_dlevels = [];
    let page_rlevels = [];

    for (let i = 0; i < Math.min(opts.page_size, values.length - voff); ++i) {
      if (values[voff + i].value !== null) {
        page_values.push(values[voff + i].value);
      }

      page_rlevels.push(values[voff + i].rlevel);
      page_dlevels.push(values[voff + i].dlevel);
    }

    /* encode data page */
    pages.push(
        encodeDataPage(
            opts.column,
            values.length,
            page_values,
            page_rlevels,
            page_dlevels));
  }

  /* build column chunk header */
  let pages_buf = Buffer.concat(pages);
  metadata.total_uncompressed_size = pages_buf.length;
  metadata.total_compressed_size = pages_buf.length;
  metadata.codec =
      parquet_util.setThriftEnum(parquet_thrift.CompressionCodec, "UNCOMPRESSED");

  let metadata_offset = opts.base_offset + pages_buf.length;
  let body = Buffer.concat([pages_buf, parquet_util.serializeThrift(metadata)]);
  return { body, metadata, metadata_offset };
}

/**
 * Encode a list of column values into a parquet row group
 */
function encodeRowGroup(schema, row_count, rows, opts) {
  let metadata = new parquet_thrift.RowGroup();
  metadata.num_rows = row_count;
  metadata.columns = [];
  metadata.total_byte_size = 0;

  let body = Buffer.alloc(0);
  schema.columns.forEach(function(col_def) {
    let cchunk_data = encodeColumnChunk({
      column: col_def,
      values: rows[col_def.name],
      base_offset: opts.base_offset + body.length,
      page_size: opts.page_size,
      encoding: col_def.encoding
    });

    let cchunk = new parquet_thrift.ColumnChunk();
    cchunk.file_offset = cchunk_data.metadata_offset;
    cchunk.meta_data = cchunk_data.metadata;
    metadata.columns.push(cchunk);
    metadata.total_byte_size += cchunk_data.body.length;

    body = Buffer.concat([body, cchunk_data.body]);
  });

  return { body, metadata };
}

/**
 * Encode a parquet file metadata footer
 */
function encodeFooter(schema, row_count, row_groups) {
  let fmd = new parquet_thrift.FileMetaData()
  fmd.version = PARQUET_VERSION;
  fmd.created_by = "parquet.js";
  fmd.num_rows = row_count;
  fmd.row_groups = row_groups;
  fmd.schema = [];

  {
    let schema_root = new parquet_thrift.SchemaElement();
    schema_root.name = "root";
    schema_root.num_children = schema.columns.length;
    fmd.schema.push(schema_root);
  }

  schema.columns.forEach(function(col_def) {
    let tcol = new parquet_thrift.SchemaElement();
    tcol.name = col_def.name;
    tcol.type = parquet_util.setThriftEnum(parquet_thrift.Type, col_def.type);
    tcol.repetition_type = parquet_util.setThriftEnum(
        parquet_thrift.FieldRepetitionType,
        col_def.repetition_type);

    fmd.schema.push(tcol);
  });

  let fmd_bin = parquet_util.serializeThrift(fmd);

  let footer = new Buffer(fmd_bin.length + 8);
  fmd_bin.copy(footer);
  footer.writeUInt32LE(fmd_bin.length, fmd_bin.length);
  footer.write(PARQUET_MAGIC, fmd_bin.length + 4);
  return footer;
}

module.exports = {
  ParquetWriter,
  ParquetFileWriter,
};

