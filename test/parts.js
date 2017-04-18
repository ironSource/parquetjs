
function pullBits (value, width, bits) {
  return (((0xff >> (8 - width)) << width) & (value << bits)) >> width
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

function bits (width) {
  var w = 0, m = steps(width)
  //console.log('bytes:', m, 'bits:', m*8)
  var a = []
  var byte = 0
  while(w < m*8) {
    //does adding this overlap a byte?
    //the bits left in this item to be added.
    var _w = width// - Math.min(w % 8, width)
    if((w+1) >> 3 !== (w + _w) >> 3) {
     var overflow = ((w + _w) % 8)
      a.push(_w - overflow)
      if(overflow)
        a.push(overflow)
    }
    else {
      a.push(_w)
    }
    w += _w //Math.min(w + width, byte + 8)
  }
  console.log(a)
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
  for(var i = 1; i <= 12; i++)
    bits(i)

  t.end()
})




















