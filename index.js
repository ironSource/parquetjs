var RLE = require('./run-length-encoding')
var bitpacking = require('bitpacking')
var varint = require('varint')
var assert = require('assert')

function append(ary, item) {
  ary.push(item)
  return ary
}

function h (width) {
  var runs = []
  var buffer = []
  var previous = 0
  var repeats = 1
  function appendRLE () {
    runs.push({value: previous, repeats: repeats})
  }

  function appendBitpack () {
    var last = runs.length - 1
    if(Array.isArray(runs[last]))
      runs[last] = runs[last].concat(buffer)
    else
      runs.push(buffer)
    repeats = 0
    buffer = []
  }

  return {
    write: function (value) {
      if(value === previous) {
        repeats ++
        //do not add to buffer, because we will RLE
        if(repeats >= 8) return
      }
      else {
        if(repeats >= 8)
          appendRLE()
        repeats = 1
        previous = value
      }
      buffer.push(value)
      if(buffer.length == 8)
        appendBitpack()
    },
    end: function () {
      if(repeats >= 8)
        appendRLE()
      else if(buffer.length > 0) {
        while(buffer.length < 8)
          buffer.push(0)
        appendBitpack()
      }
      return runs
    }
  }
}

function toRuns (input) {
  var _h = h()
  input.forEach(function (value) {
    _h.write(value)
  })
  return _h.end()


  var runs = []
  var buffer = []
  var previous = null
  var repeats = 1
  input.forEach(function (value) {
    if(previous == null) {
      previous = value
    } else if(previous === value) {
      repeats ++
      if(repeats >= 8) return
    } else {
      //new value, but maybe the end of a rle run
      if(repeats >= 8)
        runs.push({value: previous, repeats: repeats})
      repeats = 1
    }

    if(repeats < 8) {
      buffer.push(value)
      previous = value
      if(buffer.length >= 8) {
        //if the previous buffer
        appendBitpack()
      }
    }
  })

  function appendBitpack () {
    var last = runs.length - 1
    console.log('last', runs[last])
    if(Array.isArray(runs[last])) {
//      console.log('CONCAT', runs, buffer)
      runs[last] = runs[last].concat(buffer)
    } else
      runs.push(buffer)
    buffer = []
  }

  if(repeats >= 8)
    runs.push({value: previous, repeats: repeats})
  else
    appendBitpack()
  return runs
}

module.exports = function (width) {
  function encode (inputs, buf, offset) {
    offset = offset | 0
    var bytes = 0
    //count number of repeats
    var i = 0
    while(i < inputs.length) {
      var _i = i++
      var value = inputs[_i]
      for(; i < inputs.length && inputs[i] === value; i++)
        ;
      if(i > 1) {
        RLE.encode({value: value, repeats: i - _i, width: width}, buf, offset + bytes)
        bytes += RLE.encode.bytes
      }
      else {
        //decide how many items to bitpack.
        var packLength = Math.min(inputs.length - i, 504)
        console.log("BITPACK RUN", inputs.length - i, packLength, inputs.length)
        var header = (Math.ceil(packLength / 8) << 1) | 1

        buf[offset] = header //this is always a singe byte
        //varint.encode(header, buf, offset)
        bytes += 1

        var bitpacked = inputs.slice(i - 1, packLength)
        bitpacking.LE(buf, offset, bitpacked, width)
        //_buf.copy(buf, offset + bytes)
        //console.log(_buf.toString('hex'))
        bytes += Math.ceil((bitpacked.length*width)/8)<<3 //_buf.length
        i += packLength-1
      }
    }
    if(i != inputs.length)
      throw new Error('did not encode all items, upto:'+i)

    console.log("CAP", offset + bytes, buf.length, buf[offset+bytes])
    buf[offset + bytes] = 0xff
    encode.bytes = bytes + 1
    return buf
  }
  function decode (buf, offset) {

  }

  return {
    encode: encode, decode: decode
  }
}

module.exports.bitpackRun = function (buf, offset, inputs, width) {
  var packLength = Math.min(inputs.length , 504)
  var bytes = 0
  console.log("BITPACK RUN", inputs.length, packLength, inputs.length)
  var header = (Math.ceil(packLength / 8) << 1) | 1
//  assert.equal(header % 8, 0) //always a multiple of 8
  buf[offset++] = header //this is always a singe byte
  bytes += 1
  console.log('HEADER', buf, header.toString(16))

  bitpacking.LE(buf, offset, inputs, width)
  bytes += Math.ceil((inputs.length*width)/8)<<3 //_buf.length
  return buf
}

module.exports.runLengthRun = function (buf, offset, value, repeats, width) {
  return RLE.encode({value: value, repeats: repeats, width: width}, buf, offset)
}

module.exports.runs = toRuns










