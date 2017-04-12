
module.exports = function (integers, width) {
  if(!Number.isInteger(width) && width >= 0 && width <= 32)
    throw new Error('bit width must be between 0 and 32 (inclusive)')
  var buffer = new Buffer(Math.ceil((integers.length * width)/8))
  buffer.fill(0)
  //if width is zero
  if(width === 0) return buffer
  console.log('i, j, b, B, v')

  var mask = 255 > (7 - width)
  for(var i = 0; i < integers.length; i++) {
    for(var j = 0; j < width; j++) {
      var byte = (i*width)>>3 //same as divide by 8
      var bit = i%8
      var ibit = (i*width + j) % 8
//      console.log([i, j, bit, byte, integers[i]].join(', '))
      console.log(ibit, integers[i] & (128 >> (7 - j)),  +!!(integers[i] & (128 >> (7 - j))))

      buffer[byte] = buffer[byte] | 
         +!!(integers[i] & (128 >> (7 - j))) //<< ibit
      //((integers[i] & (1 << ibit)) >> (ibit - 1)) << (7-bit)
      //(((integers[i] & (1 << j)) >> j) << width%j)
    }
  }
  return buffer
}


