
var setBits = require('buffer-bits')
//
//module.exports = function (width) {
//
//  return {
//    encode: function (array, out, offset) {
//      encode(out, offset, array, width)
//    },
//    decode: function (in, offset) {
//      throw new Error('bitpacking.decode: not yet implemented')
//    },
//    encodingLength: function (array) {
////      array.length * width
//    }
//  }
//
//}
//
module.exports = function (_, array, width) {
//  offset = offset | 0
  var bits = array.length * width
  var offset = 0
//  if(!buffer) {
    buffer = new Buffer(Math.ceil(bits / 32)*4)
    buffer.fill(0)
  //}

  for(var i = 0; i < array.length; i++)
    setBits(buffer, array[i], width, offset*8 + i*width)


  return buffer.slice(0, Math.ceil(bits/8))
}




