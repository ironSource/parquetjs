'use strict';
const thrift = require('thrift');

/**
 * Helper function that serializes a thrift object into a buffer
 */
exports.serializeThrift = function(obj) {
  var output = []

  var transport = new thrift.TBufferedTransport(null, function (buf) {
    output.push(buf)
  })

  var protocol = new thrift.TCompactProtocol(transport)
  obj.write(protocol)
  transport.flush()

  return Buffer.concat(output)
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
  var n = klass[value];
  if (n == 0) {
    return value;
  } else {
    return n;
  }
}

