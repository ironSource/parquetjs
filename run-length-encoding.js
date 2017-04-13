var varint = require('varint')


exports.encode = function encode (opts, buffer, offset) {
  offset = offset | 0
  var byteWidth = Math.ceil(opts.width/8)
  var header = opts.repeats << 1
  var value = opts.value
  buffer = buffer || new Buffer(
    varint.encodingLength(header)
  + byteWidth
  )
  console.log('header', header, buffer, offset)
  varint.encode(header, buffer, offset)
  var bytes = varint.encode.bytes
  var index = bytes + offset
  if(byteWidth === 0)
    encode.bytes = bytes
  else if(byteWidth === 1) {
    console.log('write one byte', buffer, index, value)
    buffer[index] = value
  }
  else if(byteWidth === 2) {
    buffer.writeUIntLE(value, index)
  }
  else if(byteWidth === 3) {
    buffer.writeInt16LE(value & 0xffff, index)
    buffer[index + 2] = value >> 16
  }
  else if(byteWidth === 4) {
    buffer.writeInt32LE(value, index)
  }
  else
    throw new Error('byteWidth > 4 not implemented')

  encode.bytes = bytes + byteWidth
  return buffer
}




