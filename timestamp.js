var Int53 = require('int53')
var julian = require('julian')
console.log(Int53)
// implements parquet timestamp, as per
// which 8 bytes of little endian nano seconds since start of julian day
// then 4 bytes of little endian julian day. https://en.wikipedia.org/wiki/Julian_day
// https://github.com/prestodb/presto/blob/c73359fe2173e01140b7d5f102b286e81c1ae4a8/presto-hive/src/main/java/com/facebook/presto/hive/parquet/ParquetTimestampUtils.java#L52-L54

exports.encode = function (timestamp) {
  if(isNaN(+timestamp)) throw new Error('expected timestamp, got NaN')
  var buffer = new Buffer(12)
  var ms_day = timestamp % 86400000
  var ns_day = ms_day * 1000000
  console.log('nano', ns_day)
  Int53.writeUInt64LE(ns_day, buffer, 0)

  buffer.writeInt32LE(+julian(timestamp - ms_day), 8)
  return buffer
}
exports.decode = function (buffer) {
  var julian_day = buffer.readInt32LE(8) //or should it be signed?
  var nano = Int53.readUInt64LE(buffer, 0)
  //convert to ms since midnight
    console.log('j d ', julian_day)
  console.log('juli', +julian.toDate(julian_day))
  console.log('nano     ', nano/1000000)
  var ms = nano / 1000000
  return ms + (+julian.toDate(julian_day))
}



