var RLE = require('./run-length-encoding')

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
        console.log('offset', offset, bytes, offset + bytes)
        RLE.encode({value: value, repeats: i - _i, width: width}, buf, offset + bytes)
        bytes += RLE.encode.bytes
      }
      else
        throw new Error('expected repeat')
    }
    if(i != inputs.length) throw new Error('did not encode all items')

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












