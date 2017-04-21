

var tape = require('tape')

var runs = require('../').runs

function seq (n, fn) {
  var items = []
  for(var i = 0; i < n; i++)
    items.push(fn(i))
  return items
}

tape('rle only', function (t) {
  var actual = runs(seq(100, function () { return 4 }))
  t.deepEqual(actual, [{value: 4, repeats: 100}])

  t.end()
})

tape('short bitpack', function (t) {
  var input = seq(5, function (i) { return i % 3 })
  var actual = runs(input)
  t.deepEqual(actual, [[0,1,2,0,1,0,0,0]])

  t.end()
})



tape('bitpack only', function (t) {
  var input = seq(10, function (i) { return i % 3 })
  var actual = runs(input)
  t.deepEqual(actual, [[0,1,2,0,1,2,0,1,2,0,0,0,0,0,0,0]])

  t.end()
})

tape('bitpack/rle', function (t) {
  var input =
    seq(5, function (i) { return i % 3 })
    .concat(seq(100, function () { return 5 }))
  var actual = runs(input)
  t.deepEqual(actual, [[0,1,2,0,1,5,5,5], {value: 5, repeats: 97}])

  t.end()
})


