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
    fs.open(filePath, 'r', (err, fd) => {
      if (err) {
        callback(err, null);
      } else {
        let readFn = function(buffer, position, length, callback) {
          fs.read(fd, buffer,0, length, position, callback);
        };

        let closeFn = function(callback) {
          fs.close(fd, callback);
        };

        let reader = new ParquetReader(readFn, closeFn);
        reader.verifyHeader(function(err, res) {
          if (err) {
            callback(err, null);
          } else if (res) {
            callback(null, reader);
          } else {
            callback('not a valid parquet file', null);
          }
        });
      }
    });
  }

  constructor(readFn, closeFn) {
    this.read = readFn;
    this.close = closeFn;
  }

  verifyHeader(callback) {
    this.read(
        Buffer.alloc(PARQUET_MAGIC.length),
        0,
        PARQUET_MAGIC.length,
        (err, bytesRead, buffer) => {
      if (err) {
        callback(err, false);
      } else if (bytesRead != PARQUET_MAGIC.length) {
        callback("error reading parquetmagic", false);
      } else if (buffer.toString() == PARQUET_MAGIC) {
        callback(null, true);
      } else {
        callback("invalid parquet magic", false);
      }
    });
  }

}

module.exports = {
  ParquetReader,
};

