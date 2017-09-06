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
 * Write a parquet file to an output stream. The ParquetWriter will perform
 * buffering/batching for performance, so close() must be called after all rows
 * are written.
 */
class ParquetWriter {

  /**
   * Create a new buffered parquet writer that writes to the specified file
   */
  static openFile(schema, path, opts, callback) {
    ParquetEnvelopeWriter.openFile(schema, path, opts, (err, envelopeWriter) => {
      if (err) {
        return callback(err, null);
      }

      envelopeWriter.writeHeader((err) => {
        if (err) {
          envelopeWriter.close();
          callback(err, null);
        } else {
          callback(null, new ParquetWriter(schema, envelopeWriter));
        }
      });
    });
  }

  /**
   * Create a new buffered parquet writer for a given envelope writer
   */
  constructor(schema, envelopeWriter) {
    this.schema = schema;
    this.envelopeWriter = envelopeWriter;
    this.rowBuffer = {};
    this.rowGroupSize = PARQUET_DEFAULT_ROW_GROUP_SIZE;
    this.closed = false;
  }

  /**
   * Append a single row to the parquet file. Rows are buffered in memory until
   * rowGroupSize rows are in the buffer or close() is called
   */
  appendRow(row, callback) {
    if (!callback) {
      callback = () => {};
    }

    if (this.closed) {
      throw 'writer was closed';
    }

    parquet_shredder.shredRecord(this.schema, row, this.rowBuffer);

    if (this.rowBuffer.rowCount >= this.rowGroupSize) {
      this.envelopeWriter.writeRowGroup(this.rowBuffer, callback);
      this.rowBuffer = {};
    } else {
      callback(null);
    }
  }

  /**
   * Finish writing the parquet file and commit the footer to disk. This method
   * MUST be called after you are finished adding rows. You must not call this
   * method twice on the same object or add any rows after the close() method has
   * been called
   */
  close(callback) {
    if (!callback) {
      callback = () => {};
    }

    if (this.closed) {
      throw 'writer was closed';
    }

    this.closed = true;
    let closeFinally = () => {
      this.envelopeWriter.writeFooter((err) => {
        if (err) {
          callback(err);
        } else {
          this.envelopeWriter.close(callback);
          this.envelopeWriter = null;
        }
      });
    };

    if (this.rowBuffer.rowCount > 0) {
      this.envelopeWriter.writeRowGroup(this.rowBuffer, (err) => {
        if (err) {
          callback(err);
        } else {
          closeFinally(err);
        }
      });
      this.rowBuffer = {};
    } else {
      closeFinally();
    }
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
 * Create a parquet file from a schema and a number of row groups. This class
 * performs direct, unbuffered writes to the underlying output stream and is
 * intendend for advanced and internal users; the writeXXX methods must be
 * called in the correct order to produce a valid file.
 */
class ParquetEnvelopeWriter {

  /**
   * Create a new parquet envelope writer that writes to the specified file
   */
  static openFile(schema, path, opts, callback) {
    let outputStream = fs.createWriteStream(path, opts);

    outputStream.on('open', function(fd) {
      let writeFn = function(buf, callback) {
        outputStream.write(buf, callback);
      }

      let closeFn = function(callback) {
        outputStream.end(callback);
      }

      callback(null, new ParquetEnvelopeWriter(schema, writeFn, closeFn, 0));
    });

    outputStream.on('error', function(err) {
      callback(err, null);
    });
  }

  constructor(schema, writeFn, closeFn, fileOffset) {
    this.schema = schema;
    this.write = writeFn,
    this.close = closeFn;
    this.offset = fileOffset;
    this.rowCount = 0;
    this.rowGroups = [];
    this.pageSize = PARQUET_DEFAULT_PAGE_SIZE;
  }

  writeSection(buf, callback) {
    this.offset += buf.length;
    this.write(buf, callback);
  }

  /**
   * Encode the parquet file header
   */
  writeHeader(callback) {
    this.writeSection(Buffer.from(PARQUET_MAGIC), callback);
  }

  /**
   * Encode a parquet row group. The records object should be created using the
   * shredRecord method
   */
  writeRowGroup(records, callback) {
    let rgroup = encodeRowGroup(this.schema, records.rowCount, records.columnData, {
      baseOffset: this.offset,
      pageSize: this.pageSize
    });

    this.rowCount += records.rowCount;
    this.rowGroups.push(rgroup.metadata);
    this.writeSection(rgroup.body, callback);
  }

  /**
   * Write the parquet file footer
   */
  writeFooter(callback) {
    if (this.rowCount == 0) {
      throw 'cannot write parquet file with zero rows';
    }

    if (this.schema.columns.length == 0) {
      throw 'cannot write parquet file with zero columns';
    }

    this.writeSection(
        encodeFooter(this.schema, this.rowCount, this.rowGroups),
        callback);
  };

  /**
   * Set the parquet data page size. The data page size controls the maximum 
   * number of column values that are written to disk as a consecutive array
   */
  setPageSize(cnt) {
    this.pageSize = cnt;
  }

}

/**
 * Encode a parquet data page
 */
function encodeDataPage(column, valueCount, values, rlevels, dlevels) {
  /* encode values */
  let valuesBuf;
  switch (column.encoding) {

    case 'PLAIN':
      valuesBuf = parquet_codec_plain.encodeValues(column.primitiveType, values);
      break;

    case 'RLE':
      valuesBuf = parquet_codec_rle.encodeValues(column.primitiveType, values);
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
      { bitWidth: parquet_util.getBitWidth(column.dLevelMax) });

  /* build page header */
  let pageBody = Buffer.concat([rLevelsBuf, dLevelsBuf, valuesBuf]);
  let pageHeader = new parquet_thrift.PageHeader()
  pageHeader.num_values = valueCount;
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
  metadata.type = parquet_util.setThriftEnum(parquet_thrift.Type, opts.column.primitiveType);
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
    let pageRLevels = [];
    let pageDLevels = [];

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
    schemaElem.type = parquet_util.setThriftEnum(parquet_thrift.Type, colDef.primitiveType);
    schemaElem.repetition_type = parquet_util.setThriftEnum(
        parquet_thrift.FieldRepetitionType,
        colDef.repetition_type);

    if (colDef.convertedType) {
      schemaElem.converted_type =
          parquet_util.setThriftEnum(parquet_thrift.ConvertedType, colDef.convertedType);
    }

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
  ParquetEnvelopeWriter,
  ParquetWriter,
};

