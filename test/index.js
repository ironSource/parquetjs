
//these examples from from parquet-mr/parquet-encoding
var fixtures = require('./fixture.json')
var tape = require('tape')

tape('fixtures', function (t) {
  fixtures.forEach(function (e, i) {
    t.test('test:'+i, function (t) {
      console.log(e)
      t.end()
    })
  })
  t.end()
})



