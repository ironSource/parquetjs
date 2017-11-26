'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet_codec_rle = require('../lib/codec/rle.js');

describe('ParquetCodec::RLE', function() {

  it('should encode bitpacked values', function() {
    let buf = parquet_codec_rle.encodeValues(
        'INT32',
        [0, 1, 2, 3, 4, 5, 6, 7],
        {
          disableEnvelope: true,
          bitWidth: 3
        });

    assert.deepEqual(buf, new Buffer([0x03, 0x88, 0xc6, 0xfa]));
  });

  it('should decode bitpacked values', function() {
    let vals = parquet_codec_rle.decodeValues(
        'INT32',
        {
          buffer: new Buffer([0x03, 0x88, 0xc6, 0xfa]),
          offset: 0,
        },
        8,
        {
          disableEnvelope: true,
          bitWidth: 3
        });

    assert.deepEqual(vals, [0, 1, 2, 3, 4, 5, 6, 7]);
  });

  it('should encode repeated values', function() {
    let buf = parquet_codec_rle.encodeValues(
        'INT32',
        [42, 42, 42, 42, 42, 42, 42, 42],
        {
          disableEnvelope: true,
          bitWidth: 6
        });

    assert.deepEqual(buf, new Buffer([0x10, 0x2a]));
  });

  it('should decode repeated values', function() {
    let vals = parquet_codec_rle.decodeValues(
        'INT32',
        {
          buffer: new Buffer([0x10, 0x2a]),
          offset: 0,
        },
        8,
        {
          disableEnvelope: true,
          bitWidth: 3
        });

    assert.deepEqual(vals, [42, 42, 42, 42, 42, 42, 42, 42]);
  });

  it('should encode mixed runs', function() {
    let buf = parquet_codec_rle.encodeValues(
        'INT32',
        [0, 1, 2, 3, 4, 5, 6, 7, 4, 4, 4, 4, 4, 4, 4, 4, 0, 1, 2, 3, 4, 5, 6, 7],
        {
          disableEnvelope: true,
          bitWidth: 3
        });

    assert.deepEqual(buf, new Buffer([0x03, 0x88, 0xc6, 0xfa, 0x10, 0x04, 0x03, 0x88, 0xc6, 0xfa]));
  });

  it('should decode mixed runs', function() {
    let vals = parquet_codec_rle.decodeValues(
        'INT32',
        {
          buffer: new Buffer([0x03, 0x88, 0xc6, 0xfa, 0x10, 0x04, 0x03, 0x88, 0xc6, 0xfa]),
          offset: 0,
        },
        24,
        {
          disableEnvelope: true,
          bitWidth: 3
        });

    assert.deepEqual(
        vals,
        [0, 1, 2, 3, 4, 5, 6, 7, 4, 4, 4, 4, 4, 4, 4, 4, 0, 1, 2, 3, 4, 5, 6, 7]);
  });

});
