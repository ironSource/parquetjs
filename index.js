
function pullBits (value, width, bits) {
  return ((0xffffffff >>> (32 - width)) & (value >>> (width - bits)))
}

function BE (output, offset, _, value, add, width) {
  output[offset] = (output[offset] << add) | pullBits(value, width, add)
  return value << add
}

function LE (output, offset, w, value, add, width) {
  output[offset] = output[offset] | ((value) << w%8)
  return value >> add //shift across so the next pullBits will get the top bits.
}


function _encode (output, offset, input, width, each) {
  console.log(offset, input, width)
  var w = 0
  var byte = 0
  var i = 0
  offset = offset | 0
  if(!output) {
    output = new Buffer(Math.ceil(input.length*width/8))
    output.fill(0)
  }
  while(i < input.length || w%8) {
    var value = input[i++] | 0
    //does adding this overlap a byte?
    //the bits left in this item to be added.
    var bits = 0, byte = output[offset + (w>>3)]
    while(bits < width) {
      //bits needed to complete this byte
      var bitsNeeded = 8 - (w % 8), add = 0

      if(bitsNeeded < (width - bits))
        add = bitsNeeded
      else
        add = width - bits

      value = each(output, offset + (w>>3), w, value, add, width)

      w+=add
      bits += add
    }
  }
  return output
}

function decode (input, offset, width) {
  throw new Error('not implemented yet!')
  var w = 0
  var output = []
  while(w < input.length + offset) {
    var b = 0
    var value = 0
    //read one value
    while(b < width) {
      if(width - b >= 8) {
        value = input[offset + (w>>3)]
      }
    }
  }
}

module.exports = function (output, offset, input, width) {
  return _encode(output, offset, input, width, BE)
}

module.exports.LE = function (output, offset, input, width) {
  return _encode(output, offset, input, width, LE)
}

module.exports.pullBits = pullBits
//module.exports.steps = steps



