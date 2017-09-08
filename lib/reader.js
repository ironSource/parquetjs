'use strict';
const fs = require('fs');
const thrift = require('thrift');
const parquet_thrift = require('../gen-nodejs/parquet_types')
const parquet_util = require('./util')

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = 'PAR1';

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 1;

/**
 * A parquet reader allows retrieving the rows from a parquet file in order.
 * The basic usage is to create a reader and then call getNextRow() until all
 * rows have been read. It is important that you call close() after you are
 * finished reading the file to avoid leaking file descriptors.
 */
class ParquetReader {

  static async openFile(filePath) {
    let envelopeReader = await ParquetEnvelopeReader.openFile(filePath);

    try {
      await envelopeReader.readHeader();
      let metadata = await envelopeReader.readFooter();
      return new ParquetReader(metadata, envelopeReader);
    } catch (err) {
      await envelopeReader.close();
      throw err;
    }
  }

  constructor(metadata, envelopeReader) {
    if (metadata.version != PARQUET_VERSION) {
      throw "invalid parquet version";
    }

    this.metadata = metadata;
    this.envelopeReader = envelopeReader;
  }

  getRowCount() {
    return this.metadata.num_rows;
  }

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

  static async openFile(filePath) {
    let fileStat = await parquet_util.fstat(filePath);
    let fileDescriptor = await parquet_util.fopen(filePath);

    let readFn = parquet_util.fread.bind(undefined, fileDescriptor);
    let closeFn = parquet_util.fclose.bind(undefined, fileDescriptor);

    return new ParquetEnvelopeReader(readFn, closeFn, fileStat.size);
  }

  constructor(readFn, closeFn, fileSize) {
    this.read = readFn;
    this.close = closeFn;
    this.fileSize = fileSize;
  }

  async readHeader() {
    let buf = await this.read(0, PARQUET_MAGIC.length);

    if (buf.toString() != PARQUET_MAGIC) {
      throw 'not valid parquet file'
    }
  }

  async readFooter() {
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

module.exports = {
  ParquetEnvelopeReader,
  ParquetReader,
};

