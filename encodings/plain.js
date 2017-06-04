
exports.decode = function (buffer, offset) {
  offset = offset || 0
  var ary = []
  while(offset < buffer.length) {
    var len = buffer.readUInt32LE(offset)
    ary.push(buffer.toString('utf8', offset+=4, offset+=len))
  }
  return ary
}

exports.encode = function (ary) {
  var length = 0, lengths = []
  for(var i = 0; i < ary.length; i++) {
    var _length = Buffer.byteLength(ary[i])
    lengths.push(_length)
    length += _length + 4
  }
  var buf = new Buffer(length)
  var offset = 0, i = 0
  for(var i = 0; i < ary.length; i++) {
    buf.writeUInt32LE(lengths[i], offset)
    buf.write(ary[i], offset+=4)
    offset += lengths[i]
  }
  return buf
}

