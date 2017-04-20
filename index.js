function pullBits (value, width, bits) {
  return ((0xffffffff >>> (32 - width)) & (value >>> (width - bits)))
}

function pullBitsLE (value, width, bits) {
  return ((0xffffffff >>> (32 - width)) & (value >>> (width - bits)))
}

//how many whole bytes will be needed to output a pack
//does not give correct results if width > 8 on some values (16 bits)
function steps (width) {
  var m = width
  //finding the lowest factor of width*8
  while(!(m&1)) m = m >>> 1
  return m
}

function encode (output, offset, input, width) {
  var w = 0
  var byte = 0
  var i = 0
  if(!output) output = new Buffer(Math.ceil(input.length*width/8))

  while(i < input.length) {
    var value = input[i++]
    //does adding this overlap a byte?
    //the bits left in this item to be added.
    var bits = 0, byte = output[w>>3]
    while(bits < width) {
      //bits needed to complete this byte
      var bitsNeeded = 8 - (w % 8), add = 0

      if(bitsNeeded < (width - bits))
        add = bitsNeeded
      else
        add = width - bits

      output[w>>3] = output[w>>3] | 0
      output[w>>3] = (output[w>>3] << add) | pullBits(value, width, add)
      value = value << add //shift across so the next pullBits will get the top bits.

      w+=add
      bits += add
    }
  }
  return output
}

function decode (input, offset, width) {
  var w = 0
  var output = []
  while(w < input.length + offset) {
    var b = 0
    var value = 0
    //read one value
    while(b < width) {
      if(width - b >= 8) {
        value = input[w >> 3]
      }
    }
  }
}

encode.pullBits = pullBits
encode.pullBitsLE = pullBitsLE
encode.steps = steps
module.exports = encode

module.exports.LE = function (output, offset, input, width) {
  var w = 0
  var byte = 0
  var i = 0
  if(!output) output = new Buffer(Math.ceil(input.length*width/8))

  while(i < input.length) {
    var value = input[i++]
    //does adding this overlap a byte?
    //the bits left in this item to be added.
    var bits = 0, byte = output[w>>3]
    while(bits < width) {
      //bits needed to complete this byte
      var bitsNeeded = 8 - (w % 8), add = 0

      if(bitsNeeded < (width - bits))
        add = bitsNeeded
      else
        add = width - bits

      output[w>>3] = output[w>>3] | 0
      console.log('shift:', add , w%8, 'V:'+value, (value << w%8).toString(2))

//      output[w>>3] = output[w>>3] | (pullBitsLE(value, width, add) << w%8)
//      output[w>>3] = output[w>>3] | (value << w%8)
      output[w>>3] = output[w>>3] | ((value) << w%8)
      console.log('out', output[w>>3].toString(2))
      value = value >> add //shift across so the next pullBits will get the top bits.

      w+=add
      bits += add
    }
  }
  return output

}





