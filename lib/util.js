'use strict';
const fs = require('fs');
const thrift = require('thrift');

/**
 * Helper function that serializes a thrift object into a buffer
 */
exports.serializeThrift = function(obj) {
  let output = []

  let transport = new thrift.TBufferedTransport(null, function (buf) {
    output.push(buf)
  })

  let protocol = new thrift.TCompactProtocol(transport)
  obj.write(protocol)
  transport.flush()

  return Buffer.concat(output)
}

exports.decodeThrift = function(obj, buf) {
  var transport = new thrift.TFramedTransport(buf);
  var protocol = new thrift.TCompactProtocol(transport);
  obj.read(protocol);
}

/**
 * Get the number of bits required to store a given value
 */
exports.getBitWidth = function(val) {
  if (val == 0) {
    return 0;
  } else {
    return Math.ceil(Math.log2(val + 1));
  }
}

/**
 * Workaround for a bug in the javascrip thrift impl where enums values with
 * numeric representation of zero are not correctly serialized
 */
exports.setThriftEnum = function(klass, value) {
  let n = klass[value];
  if (n == 0) {
    return value;
  } else {
    return n;
  }
}

exports.fopen = function(filePath) {
  return new Promise((resolve, reject) => {
    fs.open(filePath, 'r', (err, fd) => {
      if (err) {
        reject(err);
      } else {
        resolve(fd);
      }
    })
  });
}

exports.fstat = function(filePath) {
  return new Promise((resolve, reject) => {
    fs.stat(filePath, (err, stat) => {
      if (err) {
        reject(err);
      } else {
        resolve(stat);
      }
    })
  });
}

