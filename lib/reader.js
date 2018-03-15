'use strict';
const fs = require('fs');
const thrift = require('thrift');
const parquet_thrift = require('../gen-nodejs/parquet_types')
const parquet_shredder = require('./shred')
const parquet_util = require('./util')
const parquet_schema = require('./schema')
const parquet_codec = require('./codec')
const parquet_compression = require('./compression')
const parquet_types = require('./types');
const BufferReader = require('./bufferReader');

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = 'PAR1';

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 1;

/**
 * Internal type used for repetition/definition levels
 */
const PARQUET_RDLVL_TYPE = 'INT32';
const PARQUET_RDLVL_ENCODING = 'RLE';

/**
 * A parquet cursor is used to retrieve rows from a parquet file in order
 */
class ParquetCursor  {

  /**
   * Create a new parquet reader from the file metadata and an envelope reader.
   * It is usually not recommended to call this constructor directly except for
   * advanced and internal use cases. Consider using getCursor() on the
   * ParquetReader instead
   */
  constructor(metadata, envelopeReader, schema, columnList) {
    this.metadata = metadata;
    this.envelopeReader = envelopeReader;
    this.schema = schema;
    this.columnList = columnList;
    this.rowGroup = [];
    this.rowGroupIndex = 0;
  }

  /**
   * Retrieve the next row from the cursor. Returns a row or NULL if the end
   * of the file was reached
   */
  async next() {
    if (this.rowGroup.length === 0) {
      if (this.rowGroupIndex >= this.metadata.row_groups.length) {
        return null;
      }

      let rowBuffer = await this.envelopeReader.readRowGroup(
          this.schema,
          this.metadata.row_groups[this.rowGroupIndex],
          this.columnList);

      this.rowGroup = parquet_shredder.materializeRecords(this.schema, rowBuffer);
      this.rowGroupIndex++;
    }

    return this.rowGroup.shift();
  }

  /**
   * Rewind the cursor the the beginning of the file
   */
  rewind() {
    this.rowGroup = [];
    this.rowGroupIndex = 0;
  }
};

/**
 * A parquet reader allows retrieving the rows from a parquet file in order.
 * The basic usage is to create a reader and then retrieve a cursor/iterator
 * which allows you to consume row after row until all rows have been read. It is
 * important that you call close() after you are finished reading the file to
 * avoid leaking file descriptors.
 */
class ParquetReader {

  /**
   * Open the parquet file pointed to by the specified path and return a new
   * parquet reader
   */
  static async openFile(filePath, options) {
    let envelopeReader = await ParquetEnvelopeReader.openFile(filePath, options);
    return this.openEnvelopeReader(envelopeReader);
  }

  static async openBuffer(buffer, options) {
    let envelopeReader = await ParquetEnvelopeReader.openBuffer(buffer, options);
    return this.openEnvelopeReader(envelopeReader);
  }

  /**
   * Open the parquet file from S3 using the supplied aws client and params
   * The params have to include `Bucket` and `Key` to the file requested
   * This function returns a new parquet reader
   */
  static async openS3(client, params, options) {
    let envelopeReader = await ParquetEnvelopeReader.openS3(client, params, options);
    return this.openEnvelopeReader(envelopeReader);
  }

  /**
   * Open the parquet file from a url using the supplied request module
   * params should either be a string (url) or an object that includes 
   * a `url` property.  
   * This function returns a new parquet reader
   */
  static async openUrl(request, params, options) {
    let envelopeReader = await ParquetEnvelopeReader.openUrl(request, params, options);
    return this.openEnvelopeReader(envelopeReader); 
  }

  static async openEnvelopeReader(envelopeReader) {
    try {
      await envelopeReader.readHeader();
      let metadata = await envelopeReader.readFooter();
      return new ParquetReader(metadata, envelopeReader);
    } catch (err) {
      await envelopeReader.close();
      throw err;
    }
  }

  /**
   * Create a new parquet reader from the file metadata and an envelope reader.
   * It is not recommended to call this constructor directly except for advanced
   * and internal use cases. Consider using one of the open{File,Buffer} methods
   * instead
   */
  constructor(metadata, envelopeReader) {
    if (metadata.version != PARQUET_VERSION) {
      throw 'invalid parquet version';
    }

    this.metadata = envelopeReader.metadata = metadata;
    this.envelopeReader = envelopeReader;
    this.schema = envelopeReader.schema = new parquet_schema.ParquetSchema(
        decodeSchema(
            this.metadata.schema.splice(1)));

    /* decode any statistics values */
    if (this.metadata.row_groups) {
      this.metadata.row_groups.forEach(row => row.columns.forEach( col => {
        const stats = col.meta_data.statistics;
        if (stats) {
          const field = this.schema.findField(col.meta_data.path_in_schema);
          stats.max_value = decodeStatisticsValue(stats.max_value, field);
          stats.min_value = decodeStatisticsValue(stats.min_value, field);
        }
      }));
    }
  }

  /**
   * Return a cursor to the file. You may open more than one cursor and use
   * them concurrently. All cursors become invalid once close() is called on
   * the reader object.
   *
   * The required_columns parameter controls which columns are actually read
   * from disk. An empty array or no value implies all columns. A list of column
   * names means that only those columns should be loaded from disk.
   */
  getCursor(columnList) {
    if (!columnList) {
      columnList = [];
    }

    columnList = columnList.map((x) => x.constructor === Array ? x : [x]);

    return new ParquetCursor(
        this.metadata, 
        this.envelopeReader,
        this.schema,
        columnList);
  }

  /**
   * Return the number of rows in this file. Note that the number of rows is
   * not neccessarily equal to the number of rows in each column.
   */
  getRowCount() {
    return this.metadata.num_rows;
  }

  /**
   * Returns the ParquetSchema for this file
   */
  getSchema() {
    return this.schema;
  }

  /**
   * Returns the user (key/value) metadata for this file
   */
  getMetadata() {
    let md = {};
    for (let kv of this.metadata.key_value_metadata) {
      md[kv.key] = kv.value;
    }

    return md;
  }

  /**
   * Close this parquet reader. You MUST call this method once you're finished
   * reading rows
   */
  async close() {
    await this.envelopeReader.close();
    this.envelopeReader = null;
    this.metadata = null;
  }

}

/**
 * The parquet envelope reader allows direct, unbuffered access to the individual
 * sections of the parquet file, namely the header, footer and the row groups.
 * This class is intended for advanced/internal users; if you just want to retrieve
 * rows from a parquet file use the ParquetReader instead
 */
class ParquetEnvelopeReader {

  static async openFile(filePath, options) {
    let fileStat = await parquet_util.fstat(filePath);
    let fileDescriptor = await parquet_util.fopen(filePath);

    let readFn = parquet_util.fread.bind(undefined, fileDescriptor);
    let closeFn = parquet_util.fclose.bind(undefined, fileDescriptor);

    return new ParquetEnvelopeReader(readFn, closeFn, fileStat.size, options);
  }

  static async openBuffer(buffer, options) {
    let readFn = (offset, length) => buffer.slice(offset,offset+length);
    let closeFn = () => ({});
    return new ParquetEnvelopeReader(readFn, closeFn, buffer.length, options);
  }

  static async openS3(client, params, options) {
    let fileStat = async () => client.headObject(params).promise().then(d => d.ContentLength);

    let readFn = async (offset, length) => {
      let Range = `bytes=${offset}-${offset+length-1}`;
      let res = await client.getObject(Object.assign({Range}, params)).promise();
      return res.Body;
    };

    let closeFn = () => ({});

    return new ParquetEnvelopeReader(readFn, closeFn, fileStat, options);
  }

  static async openUrl(request, params, options) {
     if (typeof params === 'string')
      params = {url: params};
    if (!params.url)
      throw new Error('URL missing');

    params.encoding = params.encoding || null;

    let defaultHeaders = params.headers || {};

    let filesize = async () => new Promise( (resolve, reject) => {
      let req = request(params);
      req.on('response', res => {
        req.abort();
        resolve(res.headers['content-length']);
      });
      req.on('error', reject);
    });

    let readFn = (offset, length) => {
      let range = `bytes=${offset}-${offset+length-1}`;
      let headers = Object.assign({}, defaultHeaders, {range});
      let req = Object.assign({}, params, {headers});
      return new Promise( (resolve, reject) => {
        request(req, (err, res) => {
          if (err) {
            reject(err);
          } else {
            resolve(res.body);
          }
        });
      });
    };

    let closeFn = () => ({});

    return new ParquetEnvelopeReader(readFn, closeFn, filesize, options);
  }

  constructor(readFn, closeFn, fileSize, options) {
    options = options || {};
    this.readFn = readFn;
    this.close = closeFn;
    this.fileSize = fileSize;
    if (options.maxLength || options.maxSpan || options.queueWait) {
      const bufferReader = new BufferReader(this, options);
      this.read = (offset, length) => bufferReader.read(offset, length);
    }
  }

  async read(offset, length) {
    return await this.readFn(offset, length);
  }

  async readHeader() {
    let buf = await this.read(0, PARQUET_MAGIC.length);

    if (buf.toString() != PARQUET_MAGIC) {
      throw 'not valid parquet file'
    }
  }

  // Helper function to get the column object for a particular path and row_group
  getColumn(path, row_group) {
    let column;
    if (!isNaN(row_group)) {
      row_group = this.metadata.row_groups[row_group];
    }

    if (typeof path === 'string') {
      if (!row_group) {
       throw `Missing RowGroup ${row_group}`;
      }
      column = row_group.columns.find(d => d.meta_data.path_in_schema.join(',') === path);
      if (!column) {
        throw `Column ${path} Not Found`;
      }
    } else {
      column = path;
    }
    return column;
  }

  async readOffsetIndex(path, row_group) {
    let column = this.getColumn(path, row_group);
    let offset_index = new parquet_thrift.OffsetIndex();
    parquet_util.decodeThrift(offset_index,await this.read(+column.offset_index_offset, column.offset_index_length));
    Object.defineProperty(offset_index,'column', {value: column, enumerable: false});
    return offset_index;
  }

  async readColumnIndex(path, row_group) {
    let column = this.getColumn(path, row_group);
    let column_index = new parquet_thrift.ColumnIndex();
    if (!column.column_index_offset) {
      throw new Error('No Column Index');
    }
    parquet_util.decodeThrift(column_index,await this.read(+column.column_index_offset, column.column_index_length));
    Object.defineProperty(column_index, 'column', { value: column });

    // decode the statistics values
    const field = this.schema.findField(column.meta_data.path_in_schema);
    if (column_index.max_values) {
      column_index.max_values = column_index.max_values.map(max_value => decodeStatisticsValue(max_value, field));
    }
    if (column_index.min_values) {
      column_index.min_values = column_index.min_values.map(min_value => decodeStatisticsValue(min_value, field));
    }

    return column_index;
  }

  async readPage(offsetIndex, pageNumber, records) {
    let column = Object.assign({},offsetIndex.column);
    column.metadata = Object.assign({},column.metadata);
    column.meta_data.data_page_offset = offsetIndex.page_locations[pageNumber].offset;
    column.meta_data.total_compressed_size =  offsetIndex.page_locations[pageNumber].compressed_page_size;
    const chunk = await this.readColumnChunk(this.schema, column);
    Object.defineProperty(chunk,'column', {value: column});
    let data = {
      columnData: {[chunk.column.meta_data.path_in_schema.join(',')]: chunk},
      rowCount: offsetIndex.page_locations.length
    };

    return parquet_shredder.materializeRecords(this.schema, data, records);
  }

  async readRowGroup(schema, rowGroup, columnList) {
    var buffer = {
      rowCount: +rowGroup.num_rows,
      columnData: {}
    };

    for (let colChunk of rowGroup.columns) {
      const colMetadata = colChunk.meta_data;
      const colKey = colMetadata.path_in_schema;

      if (columnList.length > 0 && parquet_util.fieldIndexOf(columnList, colKey) < 0) {
        continue;
      }

      buffer.columnData[colKey] = await this.readColumnChunk(schema, colChunk);
    }

    return buffer;
  }

  async readColumnChunk(schema, colChunk) {
    if (colChunk.file_path !== null) {
      throw 'external references are not supported';
    }

    let field = schema.findField(colChunk.meta_data.path_in_schema);
    let type = parquet_util.getThriftEnum(
        parquet_thrift.Type,
        colChunk.meta_data.type);

    let compression = parquet_util.getThriftEnum(
        parquet_thrift.CompressionCodec,
        colChunk.meta_data.codec);

    let pagesOffset = +colChunk.meta_data.data_page_offset;
    let pagesSize = +colChunk.meta_data.total_compressed_size;
    let pagesBuf = await this.read(pagesOffset, pagesSize);

    return decodeDataPages(pagesBuf, {
      type: type,
      rLevelMax: field.rLevelMax,
      dLevelMax: field.dLevelMax,
      compression: compression,
      column: field
    });
  }

  async readFooter() {
    if (typeof this.fileSize === 'function') {
      this.fileSize = await this.fileSize();
    }
    let trailerLen = PARQUET_MAGIC.length + 4;
    let trailerBuf = await this.read(this.fileSize - trailerLen, trailerLen);

    if (trailerBuf.slice(4).toString() != PARQUET_MAGIC) {
      throw 'not a valid parquet file';
    }

    let metadataSize = trailerBuf.readUInt32LE(0);
    let metadataOffset = this.fileSize - metadataSize - trailerLen;
    if (metadataOffset < PARQUET_MAGIC.length) {
      throw 'invalid metadata size';
    }

    let metadataBuf = await this.read(metadataOffset, metadataSize);
    let metadata = new parquet_thrift.FileMetaData();
    parquet_util.decodeThrift(metadata, metadataBuf);
    return metadata;
  }

}

/**
 * Decode a consecutive array of data using one of the parquet encodings
 */
function decodeValues(type, encoding, cursor, count, opts) {
  if (!(encoding in parquet_codec)) {
    throw 'invalid encoding: ' + encoding;
  }

  return parquet_codec[encoding].decodeValues(type, cursor, count, opts);
}


function decodeStatisticsValue(value, column) {
  if (value === null || !value.length) {
    return undefined;
  }
  if (!column.primitiveType.includes('BYTE_ARRAY')) {
    value = decodeValues(column.primitiveType,'PLAIN',{buffer: Buffer.from(value), offset: 0}, 1, column);
    if (value.length === 1) value = value[0];
  }
  if (column.originalType) {
    value = parquet_types.fromPrimitive(column.originalType, value);
  }
  return value;
}

function decodeStatistics(statistics, column) {
  if (!statistics) {
    return;
  }
  if (statistics.min_value !== null) {
    statistics.min_value = decodeStatisticsValue(statistics.min_value, column);
  }
  if (statistics.max_value !== null) {
    statistics.max_value = decodeStatisticsValue(statistics.max_value, column);
  }

  statistics.min = statistics.min_value;
  statistics.max = statistics.max_value;

  return statistics;
}

function decodeDataPages(buffer, opts) {
  let cursor = {
    buffer: buffer,
    offset: 0,
    size: buffer.length
  };

  let data = {
    rlevels: [],
    dlevels: [],
    values: [],
    count: 0
  };

  let dictionary;

  while (cursor.offset < cursor.size) {
    const pageHeader = new parquet_thrift.PageHeader();
    cursor.offset += parquet_util.decodeThrift(pageHeader, cursor.buffer.slice(cursor.offset)); 

    const pageType = parquet_util.getThriftEnum(
        parquet_thrift.PageType,
        pageHeader.type);

    let pageData = null;
    
    
    switch (pageType) {
      case 'DATA_PAGE':
      pageHeader.data_page_header.statistics = decodeStatistics(pageHeader.data_page_header.statistics, opts.column);
        pageData = decodeDataPage(cursor, pageHeader, opts);
        
        break;
      case 'DATA_PAGE_V2':
      pageHeader.data_page_header_v2.statistics = decodeStatistics(pageHeader.data_page_header_v2.statistics, opts.column);
        pageData = decodeDataPageV2(cursor, pageHeader, opts);
        
        break;
      case 'DICTIONARY_PAGE':
        dictionary = decodeDictionaryPage(cursor, pageHeader, opts);
        continue;
      default:
        throw "invalid page type: " + pageType;
    }

    if (dictionary) {
      pageData.values = pageData.values.map(d => dictionary[d]);
    }

    for (let i = 0; i < pageData.rlevels.length; i++) {
      data.rlevels.push(pageData.rlevels[i]);
      data.dlevels.push(pageData.dlevels[i]);
      let value = pageData.values[i]
      if (value !== undefined) {
        data.values.push(value);
      }
    }
    data.count += pageData.count;

  }


  return data;
}
 
function decodeDictionaryPage(cursor, header, opts) {
  const cursorEnd = cursor.offset + header.compressed_page_size;

  let dictCursor = {
    offset: 0,
    buffer: cursor.buffer.slice(cursor.offset,cursorEnd),
    size: cursorEnd - cursor.offset
  };

  cursor.offset = cursorEnd;

  if (opts.compression && opts.compression !== 'UNCOMPRESSED') {
    let valuesBuf = parquet_compression.inflate(
        opts.compression,
        dictCursor.buffer.slice(dictCursor.offset,cursorEnd));

    dictCursor = {
      buffer: valuesBuf,
      offset: 0,
      size: valuesBuf.length
    };
  }

  return decodeValues('BYTE_ARRAY', 'PLAIN', dictCursor, header.dictionary_page_header.num_values, opts)
    .map(d => d.toString());

}

function decodeDataPage(cursor, header, opts) {
  const cursorEnd = cursor.offset + header.compressed_page_size;
  let valueCount = header.data_page_header.num_values;
  let valueEncoding = parquet_util.getThriftEnum(
      parquet_thrift.Encoding,
      header.data_page_header.encoding);

  let valuesBufCursor = cursor;
  if (opts.compression && opts.compression !== 'UNCOMPRESSED') {
    let valuesBuf = parquet_compression.inflate(
        opts.compression,
        cursor.buffer.slice(cursor.offset, cursorEnd));

    valuesBufCursor = {
      buffer: valuesBuf,
      offset: 0,
      size: valuesBuf.length
    };
    cursor.offset = cursorEnd;
  }


  
  /* read repetition levels */
  let rLevelEncoding = parquet_util.getThriftEnum(
      parquet_thrift.Encoding,
      header.data_page_header.repetition_level_encoding);

  let rLevels = new Array(valueCount);
  if (opts.rLevelMax > 0) {
    rLevels = decodeValues(
        PARQUET_RDLVL_TYPE,
        rLevelEncoding,
        valuesBufCursor,
        valueCount,
        { bitWidth: parquet_util.getBitWidth(opts.rLevelMax) });
  } else {
    rLevels.fill(0);
  }

  /* read definition levels */
  let dLevelEncoding = parquet_util.getThriftEnum(
      parquet_thrift.Encoding,
      header.data_page_header.definition_level_encoding);

  let dLevels = new Array(valueCount);
  if (opts.dLevelMax > 0) {
    dLevels = decodeValues(
        PARQUET_RDLVL_TYPE,
        dLevelEncoding,
        valuesBufCursor,
        valueCount,
        { bitWidth: parquet_util.getBitWidth(opts.dLevelMax) });
  } else {
    dLevels.fill(0);
  }
 
  /* read values */
  let valueCountNonNull = 0;
  for (let dlvl of dLevels) {
    if (dlvl === opts.dLevelMax) {
      ++valueCountNonNull;
    }
  }


  let values = decodeValues(
      opts.type,
      valueEncoding,
      valuesBufCursor,
      valueCountNonNull,
      {
        typeLength: opts.column.typeLength,
        bitWidth: opts.column.typeLength,
        disableEnvelope: opts.column.disableEnvelope
      });

  return {
    dlevels: dLevels,
    rlevels: rLevels,
    values: values,
    count: valueCount
  };
}

function decodeDataPageV2(cursor, header, opts) {
  const cursorEnd = cursor.offset + header.compressed_page_size;

  const valueCount = header.data_page_header_v2.num_rows;
  const valueCountNonNull = valueCount - header.data_page_header_v2.num_nulls;
  const valueEncoding = parquet_util.getThriftEnum(
      parquet_thrift.Encoding,
      header.data_page_header_v2.encoding);

  /* read repetition levels */
  let rLevels = new Array(valueCount);
  if (opts.rLevelMax > 0) {
    rLevels = decodeValues(
        PARQUET_RDLVL_TYPE,
        PARQUET_RDLVL_ENCODING,
        cursor,
        valueCount,
        {
          bitWidth: parquet_util.getBitWidth(opts.rLevelMax),
          disableEnvelope: true
        });
  } else {
    rLevels.fill(0);
  }

  /* read definition levels */
  let dLevels = new Array(valueCount);
  if (opts.dLevelMax > 0) {
    dLevels = decodeValues(
        PARQUET_RDLVL_TYPE,
        PARQUET_RDLVL_ENCODING,
        cursor,
        valueCount,
        {
          bitWidth: parquet_util.getBitWidth(opts.dLevelMax),
          disableEnvelope: true
        });
  } else {
    dLevels.fill(0);
  }

  /* read values */
  let valuesBufCursor = cursor;

  if (header.data_page_header_v2.is_compressed) {
    let valuesBuf = parquet_compression.inflate(
        opts.compression,
        cursor.buffer.slice(cursor.offset, cursorEnd));

    valuesBufCursor = {
      buffer: valuesBuf,
      offset: 0,
      size: valuesBuf.length
    };

    cursor.offset = cursorEnd;
  }

  let values = decodeValues(
      opts.type,
      valueEncoding,
      valuesBufCursor,
      valueCountNonNull,
      {
        typeLength: opts.column.typeLength,
        bitWidth: opts.column.typeLength
      });

  return {
    dlevels: dLevels,
    rlevels: rLevels,
    values: values,
    count: valueCount
  };
}

function decodeSchema(schemaElements) {
  let schema = {};
  schemaElements.forEach(schemaElement => {

    let repetitionType = parquet_util.getThriftEnum(
        parquet_thrift.FieldRepetitionType,
        schemaElement.repetition_type);

    let optional = false;
    let repeated = false;
    switch (repetitionType) {
      case 'REQUIRED':
        break;
      case 'OPTIONAL':
        optional = true;
        break;
      case 'REPEATED':
        repeated = true;
        break;
    };

    if (schemaElement.num_children > 0) {
      schema[schemaElement.name] = {
        optional: optional,
        repeated: repeated,
        fields: Object.create({},{
          /* define parent and num_children as non-enumerable */
          parent: {
            value: schema,
            enumerable: false 
          },
          num_children: {
            value: schemaElement.num_children,
            enumerable: false
          }
        })
      };
      /* move the schema pointer to the children */
      schema = schema[schemaElement.name].fields;
    } else {
      let logicalType = parquet_util.getThriftEnum(
          parquet_thrift.Type,
          schemaElement.type);

      if (schemaElement.converted_type != null) {
        logicalType = parquet_util.getThriftEnum(
            parquet_thrift.ConvertedType,
            schemaElement.converted_type);
      }

      schema[schemaElement.name] = {
        type: logicalType,
        typeLength: schemaElement.type_length,
        optional: optional,
        repeated: repeated
      };
    }

    /* if we have processed all children we move schema pointer to parent again */
    while (schema.parent && Object.keys(schema).length === schema.num_children) {
      schema = schema.parent;
    }
  });
  return schema;
}

module.exports = {
  ParquetEnvelopeReader,
  ParquetReader,
};

