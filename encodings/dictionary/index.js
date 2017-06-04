var varint = require('varint')
var HRLE = require('../hybrid')

function encodeRepeats(repeats, value) {
  var len = varint.encodingLength(repeats << 1)
  var b = new Buffer(4 + len + 1)
  b.writeUInt32LE(len+1, 0)
  varint.encode(repeats << 1, b, 4)
  b[4 + len] = value
  return b
}

var plain = require('../plain')
var bitWidth = require('../bitpacking/util').bits

exports.initial = function () {
  return {
    index: -1,
    count: 0,
    dictionary: {},
    values: []
  }
}

exports.reduce = function (rec, value) {
  if(rec.dictionary[value] != null)
    rec.values.push(rec.dictionary[value])
  else {
    rec.values.push(rec.dictionary[value] = ++rec.index)
  }
  //keep a count so we can see if dictionary encoding any better
  //than plain
  rec.count++
  return rec
}

exports.encodeDictionary = function (rec) {
  return plain.encode(Object.keys(rec.dictionary))
}

exports.encodeValues = function (rec) {
  //encode all values as non null.
  var width = bitWidth(rec.index)
  var h = HRLE(width)
  //XXX should figure out the right size for the buffer!
  var b = new Buffer(1024*1024)
  b.fill(0)
  h.encode(rec.values, b)
  return Buffer.concat([
    encodeRepeats(rec.values.length, 1),
    new Buffer([width]),
    b.slice(0, h.encode.bytes)
  ])
}

