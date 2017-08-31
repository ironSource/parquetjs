var RLE = require('./run-length-encoding')
var bitpacking = require('../bitpacking')
var varint = require('varint')
var assert = require('assert')

var Runs = require('./runs')

function toRuns (input) {
  var runs = Runs()
  input.forEach(function (value) {
    runs.write(value)
  })
  return runs.end()
}

function assertNaNaN (n) {
  if(isNaN(n)) throw new Error('expected not not a number')
  return n
}

function assertInteger(n) {
  if(!Number.isInteger(n)) throw new Error('expected an integer, was:'+n)
}

module.exports = function (width) {
  function encode (inputs, buf, offset) {
    offset = offset | 0
    var bytes = 0
    //if the with is zero, then only RLE makes sense
    //parquet-mr actuall does use bitpacked with zero width
    //when under 8 items, weird but just means there is not
    //exact count of repeats. using RLE would still be valid.
    if(width === 0) {
      runLengthRun(buf, offset, 0, inputs.length, 0)
      encode.bytes = runLengthRun.bytes
      return buf
    }
    var runs = toRuns(inputs)

    for(var i = 0; i < runs.length; i++) {
      if(Array.isArray(runs[i])) {
        bitpackRun(buf, assertNaNaN(offset), runs[i], width)
        bytes += bitpackRun.bytes
        offset += bitpackRun.bytes
      }
      else {
        runLengthRun(buf, assertNaNaN(offset), runs[i].value, runs[i].repeats, width)
        bytes += runLengthRun.bytes
        offset += runLengthRun.bytes
      }
    }
//    buf[offset] = 0xff

    encode.bytes = bytes
    return buf
  }
  function decode (buf, offset) {
    throw new Error('decode: not implemented yet')
  }

  return {
    encode: encode, decode: decode
  }
}

module.exports.bitpackRun = bitpackRun

function bitpackRun (buf, offset, inputs, width) {
  assertInteger(width)
  var packLength = Math.min(inputs.length , 504)
  var bytes = 0
  var header = (Math.ceil(packLength / 8) << 1) | 1
  buf[offset++] = header //this is always a singe byte

  bitpacking.LE(buf, offset, inputs, width)
  bitpackRun.bytes = 1 + Math.ceil((inputs.length*width)/8) //_buf.length
  return buf
}

module.exports.runLengthRun = runLengthRun
function runLengthRun (buf, offset, value, repeats, width) {
  assertInteger(width)
  RLE.encode({value: value, repeats: repeats, width: width}, buf, offset)
  runLengthRun.bytes = RLE.encode.bytes
  return buf
}

module.exports.runs = toRuns







