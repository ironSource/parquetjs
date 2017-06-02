var Int53 = require('int53')
var julian = require('julian')

// implements parquet timestamp, as per
// which 8 bytes of little endian nano seconds since start of julian day
// then 4 bytes of little endian julian day. https://en.wikipedia.org/wiki/Julian_day
// https://github.com/prestodb/presto/blob/c73359fe2173e01140b7d5f102b286e81c1ae4a8/presto-hive/src/main/java/com/facebook/presto/hive/parquet/ParquetTimestampUtils.java#L52-L54

var M = 1000000 //multiply milli by 1 million to get nano

exports.encode = function (timestamp, buffer, offset) {
  if(!buffer) {
    buffer = new Buffer(12)
    offset = 0
  }
  if(isNaN(+timestamp)) throw new Error('expected timestamp, got NaN')
  var buffer = new Buffer(12)
  Int53.writeUInt64LE(
    julian.toMillisecondsInJulianDay(timestamp)*M,
    buffer, offset
  )
  buffer.writeInt32LE(+julian.toJulianDay(timestamp), offset+8)
  return buffer
}
exports.decode = function (buffer, offset) {
  offset = offset || 0
  return julian.fromJulianDayAndMilliseconds(
    buffer.readInt32LE(offset+8),
    Int53.readUInt64LE(buffer, offset)/M
  )
}

if(!module.parent) {
  var n = Date.now()
  console.log(n)
  console.log(exports.encode(n))
  console.log(exports.decode(exports.encode(n)))
}

