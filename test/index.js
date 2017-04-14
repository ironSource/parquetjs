
//these examples from from parquet-mr/parquet-encoding
var fixtures = require('./fixture.json')
var tape = require('tape')
var u = require('../util')
var packer = require('../')

tape('fixtures', function (t) {
  fixtures.forEach(function (e, i) {
    t.test('test:'+i, function (t) {
//      console.log(e)
      var bytes = Math.ceil(Math.ceil((e.ary.length*e.width) / 32)*4)
      var buffer = packer(new Buffer(bytes), e.ary, e.width)
      console.log('output', buffer.length, u.bufferToBits(buffer))
      t.equal(u.bufferToBits(buffer
//        .slice(0, Math.ceil((e.ary.length*e.width)/8))
        ), e.bits)
      t.end()
    })
  })
  t.end()
})



