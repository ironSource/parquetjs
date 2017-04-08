var tape = require('tape')
var u = require('../util')

tape('byte to bits', function (t) {
  t.equal(u.byteToBits(0), '00000000')
  t.equal(u.byteToBits(255), '11111111')
  t.equal(u.byteToBits(129), '10000001')
  t.equal(u.byteToBits(66), '01000010')

  for(var i = 0; i < 256; i++)
    t.equal(parseInt(u.byteToBits(i), 2), i)
  t.end()
})



