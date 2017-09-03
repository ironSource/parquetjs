var tape = require('tape')
var hexpp = require('hexpp')

var dictionary = require('../')

tape('simple', function (t) {

  var data = [
    'apple',
    'apple',
    'banana',
    'cherry',
    'apple'
  ].reduce(dictionary.reduce, dictionary.initial())
  console.log(hexpp(dictionary.encodeDictionary(data)))
  console.log(hexpp(dictionary.encodeValues(data)))
  t.end()

})



tape('repeated', function (t) {

  var data = [
    'developers',
    'developers',
    'developers',
    'developers'
  ].reduce(dictionary.reduce, dictionary.initial())
  console.log(hexpp(dictionary.encodeDictionary(data)))
  console.log(hexpp(dictionary.encodeValues(data)))
  t.end()

})

