var u = require('../util')
function pullBits (value, width, bits) {
  return (((0xffffffff >> (32 - width)) << width) & (value << bits)) >> width
}

var tape = require('tape')

tape('get N least significant bits from the start of a number', function (t) {

  t.equal(pullBits(0, 3, 0), 0)
  t.equal(pullBits(0, 3, 1), 0)
  t.equal(pullBits(0, 3, 2), 0)
  t.equal(pullBits(0, 3, 3), 0)

  t.equal(pullBits(1, 3, 0), 0)
  t.equal(pullBits(1, 3, 1), 0)
  t.equal(pullBits(1, 3, 2), 0)
  t.equal(pullBits(1, 3, 3), 1)

  t.equal(pullBits(2, 3, 0), 0)
  t.equal(pullBits(2, 3, 1), 0)
  t.equal(pullBits(2, 3, 2), 1)
  t.equal(pullBits(2, 3, 3), 2)

  t.equal(pullBits(3, 3, 0), 0)
  t.equal(pullBits(3, 3, 1), 0)
  t.equal(pullBits(3, 3, 2), 1)
  t.equal(pullBits(3, 3, 3), 3)

  t.equal(pullBits(4, 3, 0), 0)
  t.equal(pullBits(4, 3, 1), 1)
  t.equal(pullBits(4, 3, 2), 2)
  t.equal(pullBits(4, 3, 3), 4)

  t.equal(pullBits(5, 3, 0), 0)
  t.equal(pullBits(5, 3, 1), 1)
  t.equal(pullBits(5, 3, 2), 2)
  t.equal(pullBits(5, 3, 3), 5)

  t.equal(pullBits(6, 3, 0), 0)
  t.equal(pullBits(6, 3, 1), 1)
  t.equal(pullBits(6, 3, 2), 3)
  t.equal(pullBits(6, 3, 3), 6)

  t.equal(pullBits(7, 3, 0), 0)
  t.equal(pullBits(7, 3, 1), 1)
  t.equal(pullBits(7, 3, 2), 3)
  t.equal(pullBits(7, 3, 3), 7)

  t.end()
})


tape('pullBits when value is more than one byte', function (t) {
  t.equal(pullBits(256, 9, 1), 1)
  t.end()

})

function appendBits(buffer, bitIndex, value, width) {
  for(var i = bitIndex % 8; i < 8; i += width)
    ;
}

//width  1: 1 1 1 1 1 1 1 1,
//width  2: 2 2 2 2,
//width  3: 3 3 2, 1 3 3 1, 2 3 3,
//width  4: 4 4,
//width  5: 5 3, 2 5 1, 4 4, 1 5 2, 3 5
//wydth  6: 6 2, 4 4, 2 6
//width  7: 7 1, 6 2, 5 3, 4 4, 3 5, 2 6, 1 7
//width  8: 8
//width  9: 8, 1 7, 2 6, 3 5, 4 4, 5 3, 6 2, 7 1, 8
//wydth 10: 8, 2 6, 4 4, 6 2, 8
//width 11: 8, 3 5, 6 2, 8, 1 7, 4 4, 7 1, 8, 2 6, 5 3, 8

//how many whole bytes will be needed to output a pack
function steps (width) {
  var m = width
  //finding the lowest factor of width*8
  while(!(m&1)) m = m >>> 1
//  for(var i = 0; i < width*8
  return m
}

function bits (width, input) {
  var w = 0, m = steps(width)
  var a = []
  var byte = 0
  var i = 0
  var output = new Buffer(m)
  while(w < m*8) {
    var value = 1
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

//      console.log(value, width, bits, pullBits(value, width, add))
//      console.log(value, width, add, pullBits(value, width, add))
//      console.log("ADD", w>>3, (w % 8), bitsNeeded, pullBits(value, width, add))
      output[w>>3] = output[w>>3] | 0
      output[w>>3] = (output[w>>3] << add) | pullBits(value, width, add)
//      console.log(add, value << add)
      value = value << add //shift across so the next pullBits will get the top bits.

      w+=add
      a.push(add)
      bits += add
    }
  }
  return output
//  console.log(width, m, a)
}


tape('number of bytes needed to pack width evenly', function (t) {

  t.equal(steps(1), 1)
  t.equal(steps(2), 1)
  t.equal(steps(3), 3)
  t.equal(steps(4), 1)
  t.equal(steps(5), 5)
  t.equal(steps(6), 3)
  t.equal(steps(7), 7)
  t.equal(steps(8), 1)
  t.equal(steps(9), 9)
  t.equal(steps(10), 5)
  t.equal(steps(11), 11)
  t.equal(steps(12), 3)

  t.end()
})

tape('bits', function (t) {
  for(var i = 1; i <= 32; i++)
    console.log(u.bufferToBits(bits(i)))


  t.end()
})






