

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

tape('rle/rle', function (t) {
  var actual = runs(
    seq(100, function (i) { return 4 })
    .concat(seq(100, function () { return 5 }))
  )

  t.deepEqual(actual, [{value: 4, repeats: 100}, {value:5, repeats: 100}])
  t.end()
})

tape('long bitpack', function (t) {
  var a = seq(63*8, function (i) { return 1 + i % 2 })
  var b = seq(63*8, function (i) { return 3 + i % 2 })
  var actual = runs(
       a.concat(b)
  )

  console.log(actual)
  t.deepEqual(actual, [a, b])

  t.end()
})


tape('long bitpack', function (t) {
  var a = seq(404, function (i) { return 1 + i % 2 })
  var b = seq(704, function (i) { return 3 + i % 2 })
  var c = seq(404, function (i) { return 7 + i % 2 })
  var actual = runs(
       a.concat(b).concat(c)
  )

  console.log(actual)
  t.deepEqual(actual, [
    a.concat(b.slice(0, 100)),
    b.slice(100, 604),
    b.slice(604).concat(c)
  ])

  t.end()
})

tape('short bitpack, rounded up to 8', function (t) {
  t.deepEqual(
    runs([0,1,2,3,4,5]),
    [[0,1,2,3,4,5,0,0]]
  )
  t.end()
})
