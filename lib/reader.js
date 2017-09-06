'use strict';
const fs = require('fs');
const thrift = require('thrift');
const parquet_thrift = require('../gen-nodejs/parquet_types')
const parquet_util = require('./util')

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = 'PAR1';

class ParquetReader {

  static openFile(filePath, callback) {
    ParquetDecoder.openFile(filePath, (err, decoder) => {
      decoder.readMetadata((err, metadata) => {
        if (err) {
          decoder.close();
          return callback(err, null);
        } else {
          return callback(null, new ParquetReader(metadata, decoder));
        }
      });
    });
  }

  constructor(metadata, decoder) {
    this.metadata = metadata;
    this.decoder = decoder;
  }

  close() {
    this.decoder.close();
    this.decoder = null;
    this.metadata = null;
  }

}

class ParquetDecoder {

  static openFile(filePath, callback) {
    fs.stat(filePath, (err, stat) => {
      if (err) {
        callback(err, null);
      } else {
        fs.open(filePath, 'r', (err, fd) => {
          if (err) {
            callback(err, null);
          } else {
            ParquetDecoder.openFileDescriptor(fd, stat.size, true, callback);
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

    callback(null, new ParquetDecoder(readFn, closeFn, fileSize));
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
          return callback("error reading metadata footer", null);
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
  ParquetDecoder,
  ParquetReader,
};

