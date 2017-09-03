"use strict";
var thrift = require('thrift');

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

exports.byteToBits = function (n) {
  var s = ''
  var bit = 128
//  for(var bit = 128; bit > 0; bit =>> 1)
//    s += +!!(n & bit)

  return ('' +
    (+!!(n & 128)) +
    (+!!(n & 64)) +
    (+!!(n & 32)) +
    (+!!(n & 16)) +
    (+!!(n & 8)) +
    (+!!(n & 4)) +
    (+!!(n & 2)) +
    (n & 1)
  )

  return s
}

exports.bufferToBits = function (buffer, width) {
  var s = ''
  for(var i = 0; i < buffer.length; i++)
    s += exports.byteToBits(buffer[i]) + (i + 1 < buffer.length ? ' ' : '')
  if(width != null) {
    var rx = new RegExp('((?:[01]\\s?){'+width+'})')
    console.log(rx)
    s = s.split(rx).filter(Boolean).join(',')
  }
  return s
}

