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

  static openFile(filePath, callback) {
    ParquetEnvelopeReader.openFile(filePath, (err, envelopeReader) => {
      envelopeReader.readMetadata((err, metadata) => {
        if (err) {
          envelopeReader.close();
          return callback(err, null);
        }

        try{
          callback(null, new ParquetReader(metadata, envelopeReader));
        } catch (err) {
          callback(err, null);
        }
      });
    });
  }

  constructor(metadata, envelopeReader) {
    if (metadata.version != PARQUET_VERSION) {
      throw "invalid parquet version";
    }

    this.metadata = metadata;
    this.envelopeReader = envelopeReader;
  }

  close() {
    this.envelopeReader.close();
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

  static openFile(filePath, callback) {
    fs.stat(filePath, (err, stat) => {
      if (err) {
        callback(err, null);
      } else {
        fs.open(filePath, 'r', (err, fd) => {
          if (err) {
            callback(err, null);
          } else {
            ParquetEnvelopeReader.openFileDescriptor(fd, stat.size, true, callback);
          }
        });
      }
    });
  }

  static openFileDescriptor(fileDescriptor, fileSize, autoClose, callback) {
    let readFn = function(buffer, position, length, callback) {
      fs.read(fileDescriptor, buffer, 0, length, position, callback);
    };

    let closeFn = function(callback) {
      if (fileDescriptor >= 0 && autoClose) {
        fs.close(fileDescriptor, callback);
        fileDescriptor = -1;
      }
    };

    callback(null, new ParquetEnvelopeReader(readFn, closeFn, fileSize));
  }

  constructor(readFn, closeFn, fileSize) {
    this.read = readFn;
    this.close = closeFn;
    this.fileSize = fileSize;
  }

  readMetadata(callback) {
    this.readEnvelope((err, metadataInfo) => {
      let metadataBuf = Buffer.alloc(metadataInfo.size);
      this.read(
          metadataBuf,
          metadataInfo.offset,
          metadataInfo.size,
          (err, bytesRead) => {
        if (err) {
          return callback(err, null);
        } else if (bytesRead != metadataInfo.size) {
          return callback('error reading metadata footer', null);
        }

        let metadata = new parquet_thrift.FileMetaData();
        try {
          parquet_util.decodeThrift(metadata, metadataBuf);
          callback(null, metadata);
        } catch (err) {
          callback(err, null);
        }
      });
    });
  }

  readEnvelope(callback) {
    this.readHeader((err, res) => {
      if (err || !res) {
        return callback(err ? err : 'not a valid parquet file', null);
      }

      this.readFooter(callback);
    });
  }

  readHeader(callback) {
    let buf = Buffer.alloc(PARQUET_MAGIC.length);
    this.read(
        buf,
        0,
        PARQUET_MAGIC.length,
        (err, bytesRead) => {
      if (err) {
        callback(err, false);
      } else if (bytesRead == PARQUET_MAGIC.length && buf.toString() == PARQUET_MAGIC) {
        callback(null, true);
      } else {
        callback('not a valid parquet file', false);
      }
    });
  }

  readFooter(callback) {
    let buf = Buffer.alloc(PARQUET_MAGIC.length + 4);
    this.read(
        buf,
        this.fileSize - buf.length,
        buf.length,
        (err, bytesRead) => {
      if (err) {
        return callback(err, null);
      } else if (bytesRead != buf.length) {
        return callback(Error('error reading footer trailer'), null);
      }

      if (buf.slice(4).toString() != PARQUET_MAGIC) {
        return callback(Error('not a valid parquet file'), null);
      }

      let metadataSize = buf.readUInt32LE(0);
      let metadataOffset = this.fileSize - metadataSize - buf.length;
      if (metadataOffset < PARQUET_MAGIC.length) {
        callback(Error('invalid metadata size'), null);
      } else {
        callback(null, { size: metadataSize, offset: metadataOffset });
      }
    });
  }

}

module.exports = {
  ParquetEnvelopeReader,
  ParquetReader,
};

