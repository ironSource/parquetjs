var u = require('../util')

var bp = require('../')
var pullBits = bp.pullBits
var pullBitsLE = bp.pullBitsLE
var steps = bp.steps
var tape = require('tape')

function Z(n) {
  var b = new Buffer(n); b.fill(0); return b
}

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
  for(var i = 0; i < 32; i++) {
    t.equal(pullBits(1, i+1, i), 0, 'read'+i+' bits from '+(i+1) + ' bit number')
    t.equal(pullBits(1, i+1, i+1), 1)

  }
//  t.equal(pullBits(256, 9, 1), 1)
  t.end()

})

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



tape('8x 1, to various widths', function (t) {
  var items = []
  for(var i = 0; i < 8; i++)
    items.push(1)
  for(var i = 0; i < 32; i++) {
    var out = new Buffer(((i+1)*8)>>3) //enough bytes to fit i*8 bits
    var s = u.bufferToBits(bp(out, 0, items, i+1))
    console.log(i+1, s)
    t.equal(s.replace(/[0 ]/g, '').length, 8)
  }

  t.end()
})

tape('8x 1..., to various widths', function (t) {
  for(var i = 0; i < 32; i++) {
    var items = []
    for(var j = 0; j < 8; j++)
      items.push(1<<i)
    var out = new Buffer(((i+1)*8)>>3) //enough bytes to fit i*8 bits
    var s = u.bufferToBits(bp(out, 0, items, i+1))
    console.log(i+1, s)
    t.equal(s.replace(/[0 ]/g, '').length, 8)
  }

  t.end()
})

tape('i%3 into bitwidth 3', function (t) {
  var inputs = []
  for(var i = 0; i < 20; i++)
    inputs.push(i%3)
  var out = new Buffer(Math.ceil(10*3/8))
  var bitstring = u.bufferToBits(bp(out, 0, inputs, 3), 3)
  console.log(bitstring)
  t.end()
})


//data pulled from parquet-mr tests
var BE = new Buffer('05028140a05028140a', 'hex')
var LE = new Buffer('881021428408112244','hex')

tape('little-endian', function (t) {
  var inputs = []
  for(var i = 0; i < 24; i++)
    inputs.push(i%3)

  t.deepEqual(bp.LE(Z(9), 0, inputs, 3), LE)
  t.deepEqual(bp(Z(9), 0, inputs, 3), BE)
  t.end()
})



















