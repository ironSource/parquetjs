
//these examples from from parquet-mr/parquet-encoding
var fixtures = require('./fixture.json')
var tape = require('tape')
var u = require('../util')
var packer = require('../')

function Z(n) {
  var b = new Buffer(n); b.fill(0); return b
}

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

