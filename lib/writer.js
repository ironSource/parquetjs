'use strict';
const fs = require('fs');
const thrift = require('thrift');
const parquet_thrift = require('../gen-nodejs/parquet_types')
const parquet_shredder = require('./shred')
const parquet_util = require('./util')
const parquet_codec_plain = require('./encodings/plain')
const parquet_codec_rle = require('./encodings/rle')

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = 'PAR1';

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 1;

/**
 * Default Page and Row Group sizes
 */
const PARQUET_DEFAULT_PAGE_SIZE = 8192;
const PARQUET_DEFAULT_ROW_GROUP_SIZE = 4096;

/**
 * Repetition and Definition Level Encoding
 */
const PARQUET_RDLVL_TYPE = 'INT32';
const PARQUET_RDLVL_ENCODING = 'RLE';

/**
 * Write a parquet file to an output stream. The BufferedParquetWriter will
 * perform buffering/batching for performance, so end() must be called after all
 * rows are written. Calling end() on the BufferedParquetWriter will also call
 * end() on the underlying output stream.
 */
class BufferedParquetWriter {

  /**
   * Create a new buffered parquet writer that writes to the specified file
   */
  static openFile(schema, path, opts) {
    if (!opts) {
      opts = {};
    }

    return new BufferedParquetWriter(schema, fs.createWriteStream(path, opts));
  }

  /**
   * Create a new buffered parquet writer
   */
  constructor(schema, os) {
    this.os = os;
    this.rowBuffer = [];
    this.rowGroupSize = PARQUET_DEFAULT_ROW_GROUP_SIZE;
    this.writer = new ParquetWriter(schema, this.os);
    this.writer.writeHeader();
  }

  /**
   * Append a single row to the parquet file. Rows are buffered in memory
   * until rowGroupSize rows are in the buffer or end() is called
   */
  appendRow(row) {
    if (!this.os) {
      throw 'end() was called';
    }

    this.rowBuffer.push(row);

    if (this.rowBuffer.length >= this.rowGroupSize) {
      this.writer.writeRowGroup(this.rowBuffer);
      this.rowBuffer = [];
    }
  }

  /**
   * Finish writing the parquet file and commit the footer to disk. This method
   * MUST be called after you are finished adding rows. You must not call this
   * method twice on the same object or add any rows after the end() method was called
   */
  end() {
    if (!this.os) {
      throw 'end() was called';
    }

    if (this.rowBuffer.length > 0) {
      this.writer.writeRowGroup(this.rowBuffer);
      this.rowBuffer = [];
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
    this.rowGroupSize = cnt;
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
  let osOffset = 0;
  let rowCount = 0;
  let rowGroups = [];
  let pageSize = PARQUET_DEFAULT_PAGE_SIZE;

  function write(buf) {
    osOffset += buf.length;
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
    let rowsShredded = parquet_shredder.shredRecords(schema, rows);
    let rgroup = encodeRowGroup(schema, rows.length, rowsShredded, {
      baseOffset: osOffset,
      pageSize: pageSize
    });

    rowCount += rows.length;
    rowGroups.push(rgroup.metadata);
    write(rgroup.body);
  }

  /**
   * Write the parquet file footer
   */
  this.writeFooter = function() {
    if (rowCount == 0) {
      throw 'cannot write parquet file with zero rows';
    }

    if (schema.columns.length == 0) {
      throw 'cannot write parquet file with zero columns';
    }

    write(encodeFooter(schema, rowCount, rowGroups));
  };

  /**
   * Set the parquet data page size. The data page size controls the maximum 
   * number of column values that are written to disk as a consecutive array
   */
  this.setPageSize = function(cnt) {
    pageSize = cnt;
  }

}

/**
 * Encode a parquet data page
 */
function encodeDataPage(column, value_count, values, rlevels, dlevels) {
  /* encode values */
  let valuesBuf;
  switch (column.encoding) {

    case 'PLAIN':
      valuesBuf = parquet_codec_plain.encodeValues(column.type, values);
      break;

    case 'RLE':
      valuesBuf = parquet_codec_rle.encodeValues(column.type, values);
      break;

    default:
      throw 'invalid encoding: ' + encoding;

  }

  /* encode repetition and definition levels */
  let rLevelsBuf = parquet_codec_rle.encodeValues(
      PARQUET_RDLVL_TYPE,
      rlevels,
      { bitWidth: parquet_util.getBitWidth(column.rLevelMax) });

  let dLevelsBuf = parquet_codec_rle.encodeValues(
      PARQUET_RDLVL_TYPE,
      dlevels,
      { bitWidth: parquet_util.getBitWidth(column.rLevelMax) });

  /* build page header */
  let pageBody = Buffer.concat([rLevelsBuf, dLevelsBuf, valuesBuf]);
  let pageHeader = new parquet_thrift.PageHeader()
  pageHeader.num_values = value_count;
  pageHeader.type =
      parquet_util.setThriftEnum(parquet_thrift.PageType, 'DATA_PAGE');

  pageHeader.encoding =
      parquet_util.setThriftEnum(parquet_thrift.Encoding, column.encoding);

  pageHeader.definition_level_encoding =
    parquet_util.setThriftEnum(parquet_thrift.Encoding, PARQUET_RDLVL_ENCODING);

  pageHeader.repetition_level_encoding =
      parquet_util.setThriftEnum(parquet_thrift.Encoding, PARQUET_RDLVL_ENCODING);

  pageHeader.uncompressed_page_size = pageBody.length;
  pageHeader.compressed_page_size = pageBody.length;
  pageHeader.data_page_header = new parquet_thrift.DataPageHeader();
  pageHeader.data_page_header.num_values = pageHeader.num_values;
  pageHeader.data_page_header.encoding = pageHeader.encoding;

  pageHeader.data_page_header.definition_level_encoding =
      pageHeader.definition_level_encoding;

  pageHeader.data_page_header.repetition_level_encoding =
      pageHeader.repetition_level_encoding;

  return Buffer.concat([parquet_util.serializeThrift(pageHeader), pageBody]);
}

/**
 * Encode an array of values into a parquet column chunk
 */
function encodeColumnChunk(opts) {
  let metadata = new parquet_thrift.ColumnMetaData();
  metadata.type = parquet_util.setThriftEnum(parquet_thrift.Type, opts.column.type);
  metadata.path_in_schema = opts.column.path;
  metadata.num_values = opts.values.length;
  metadata.data_page_offset = opts.baseOffset;
  metadata.encodings = [];

  /* list encodings */
  let encodingsSet = {};
  encodingsSet[PARQUET_RDLVL_ENCODING] = true;
  encodingsSet[opts.column.encoding] = true;
  for (let k in encodingsSet) {
    metadata.encodings.push(parquet_util.setThriftEnum(parquet_thrift.Encoding, k));
  }

  /* build pages */
  let values = opts.values;
  let pages = [];

  for (let voff = 0; voff <= values.length; voff += opts.pageSize) {
    /* extract page data vectors */
    let pageValues = [];
    let pageDLevels = [];
    let pageRLevels = [];

    for (let i = 0; i < Math.min(opts.pageSize, values.length - voff); ++i) {
      if (values[voff + i].value !== null) {
        pageValues.push(values[voff + i].value);
      }

      pageRLevels.push(values[voff + i].rlevel);
      pageDLevels.push(values[voff + i].dlevel);
    }

    /* encode data page */
    pages.push(
        encodeDataPage(
            opts.column,
            values.length,
            pageValues,
            pageRLevels,
            pageDLevels));
  }

  /* build column chunk header */
  let pagesBuf = Buffer.concat(pages);
  metadata.total_uncompressed_size = pagesBuf.length;
  metadata.total_compressed_size = pagesBuf.length;
  metadata.codec =
      parquet_util.setThriftEnum(parquet_thrift.CompressionCodec, 'UNCOMPRESSED');

  let metadataOffset = opts.baseOffset + pagesBuf.length;
  let body = Buffer.concat([pagesBuf, parquet_util.serializeThrift(metadata)]);
  return { body, metadata, metadataOffset };
}

/**
 * Encode a list of column values into a parquet row group
 */
function encodeRowGroup(schema, rowCount, rows, opts) {
  let metadata = new parquet_thrift.RowGroup();
  metadata.num_rows = rowCount;
  metadata.columns = [];
  metadata.total_byte_size = 0;

  let body = Buffer.alloc(0);
  for (let colDef of schema.columns) {
    let cchunkData = encodeColumnChunk({
      column: colDef,
      values: rows[colDef.name],
      baseOffset: opts.baseOffset + body.length,
      pageSize: opts.pageSize,
      encoding: colDef.encoding
    });

    let cchunk = new parquet_thrift.ColumnChunk();
    cchunk.file_offset = cchunkData.metadataOffset;
    cchunk.meta_data = cchunkData.metadata;
    metadata.columns.push(cchunk);
    metadata.total_byte_size += cchunkData.body.length;

    body = Buffer.concat([body, cchunkData.body]);
  }

  return { body, metadata };
}

/**
 * Encode a parquet file metadata footer
 */
function encodeFooter(schema, rowCount, rowGroups) {
  let metadata = new parquet_thrift.FileMetaData()
  metadata.version = PARQUET_VERSION;
  metadata.created_by = 'parquet.js';
  metadata.num_rows = rowCount;
  metadata.row_groups = rowGroups;
  metadata.schema = [];

  {
    let schemaRoot = new parquet_thrift.SchemaElement();
    schemaRoot.name = 'root';
    schemaRoot.num_children = schema.columns.length;
    metadata.schema.push(schemaRoot);
  }

  for (let colDef of schema.columns) {
    let schemaElem = new parquet_thrift.SchemaElement();
    schemaElem.name = colDef.name;
    schemaElem.type = parquet_util.setThriftEnum(parquet_thrift.Type, colDef.type);
    schemaElem.repetition_type = parquet_util.setThriftEnum(
        parquet_thrift.FieldRepetitionType,
        colDef.repetition_type);

    metadata.schema.push(schemaElem);
  }

  let metadataEncoded = parquet_util.serializeThrift(metadata);
  let footerEncoded = new Buffer(metadataEncoded.length + 8);
  metadataEncoded.copy(footerEncoded);
  footerEncoded.writeUInt32LE(metadataEncoded.length, metadataEncoded.length);
  footerEncoded.write(PARQUET_MAGIC, metadataEncoded.length + 4);
  return footerEncoded;
}

module.exports = {
  BufferedParquetWriter,
  ParquetWriter,
};

