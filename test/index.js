
//these examples from from parquet-mr/parquet-encoding
var fixtures = require('./fixture.json')
var tape = require('tape')
var u = require('../util')
var packer = require('../')

function Z(n) {
  var b = new Buffer(n); b.fill(0); return b
}

tape('simple', function (t) {
  var x80 = new Buffer([0x80])
  t.deepEqual(packer(Z(1), 0, [1], 1), x80)
  t.deepEqual(packer(Z(1), 0, [2], 2), x80)
  t.deepEqual(packer(Z(1), 0, [4], 3), x80)
  t.deepEqual(packer(Z(1), 0, [8], 4), x80)
  t.deepEqual(packer(Z(1), 0, [16], 5), x80)
  t.deepEqual(packer(Z(1), 0, [32], 6), x80)
  t.deepEqual(packer(Z(1), 0, [64], 7), x80)
  t.deepEqual(packer(Z(1), 0, [128], 8), x80)
  t.end()
})

tape('fixtures', function (t) {
  fixtures.forEach(function (e, i) {
    t.test('test:'+i, function (t) {

      var bytes = Math.ceil(Math.ceil((e.ary.length*e.width) / 32)*4)
      var buffer = packer(Z(bytes), 0, e.ary, e.width)
      var bits = e.ary.length * e.width

      console.log('output', buffer.length, u.bufferToBits(buffer))
      console.log(bits, e)

      t.equal(u.bufferToBits(
          buffer.slice(0, Math.ceil(bits/8))
        ), e.bits)
      t.end()
    })
  })
  t.end()
})

return

tape('fixtures, with default buffer', function (t) {
  fixtures.forEach(function (e, i) {
    t.test('test:'+i, function (t) {

      var bytes = Math.ceil(Math.ceil((e.ary.length*e.width) / 32)*4)
      var buffer = packer(null, 0, e.ary, e.width)
      var bits = e.ary.length * e.width

      console.log('output', buffer.length, u.bufferToBits(buffer))
      console.log(bits, e)

      t.equal(u.bufferToBits(
          buffer.slice(0, Math.ceil(bits/8))
        ), e.bits)
      t.end()
    })
  })
  t.end()
})








