//var pack = require('../')
var tape = require('tape')
var u = require('../util')
var randomBytes = require('crypto').randomBytes

tape('byte to bits', function (t) {
  t.equal(u.byteToBits(0), '00000000')
  t.equal(u.byteToBits(255), '11111111')
  t.equal(u.byteToBits(129), '10000001')
  t.equal(u.byteToBits(66), '01000010')

  for(var i = 0; i < 256; i++)
    t.equal(parseInt(u.byteToBits(i), 2), i)
  t.end()
})

tape('buffer to bytes', function (t) {
  for(var i = 0; i < 100; i++) {
    var buffer = randomBytes(32)
    var bits = u.bufferToBits(buffer)
    var _buffer = new Buffer(bits.split(' ').map(function (e) {
      return parseInt(e, 2)
    }))
    t.deepEqual(_buffer, buffer)
  }
  t.end()
})

//tape('bit pack, 1 bit wide', function (t) {
//  var packed = pack([1,1,1,1,0,0,0,0,1,1,0,0,1,0,1,0], 1)
//  console.log(u.bufferToBits(packed))
//  t.deepEqual(packed, new Buffer([128+64+32+16, 128+64+8+2]))
//  t.end()
//})
//
//tape('bit pack, 2 bits wide', function (t) {
//  var packed = pack([0,1,2,3], 2)
//  console.log(u.bufferToBits(packed))
//  t.deepEqual(packed, new Buffer([parseInt('00011011', 2)]))
//  t.end()
//})

var masks = new Array(32)
for(var i = 0; i < 32; i++)
  masks[i] = (masks[i-1] || 0) | (i == 0 ? 0 : 1 << (i-1))

console.log(masks.map(function (e) { 
  var b = new Buffer(4)
  b.writeInt32BE(e, 0)
  return b
 }))

function mask(value, width) {
  return value & (0xffffffff >>> (32 - width))
}

//var mask = 

function pack(store, value, width) {
  return (store << width) | mask(value, width)
}

function bytes32(int) {
  var b = new Buffer(4)
  b.writeInt32BE(int, 0)
  return b
}

tape('fast mask?', function (t) {
  var r = ~~(Math.random()*(1<<31))
  console.log(bytes32(r))
  for(var i = 0;i < 32; i++) {
    t.equal(mask(r, i), masks[i]&r, 'mask '+r +' width ' + i + ' is '+ (masks[i]&r))
  }
  t.end()
})

function peek(store, width) {
  return store & masks[width]
}

function shift (store, width) {
  return store >> width
}

function setBits(int, width, index) {

}

function packAll(ary, width) {
  var bytes = Math.ceil((ary.length * width) / 8)
  var ints = Math.ceil(bytes/4)
  var b = new Buffer(ints) //because we need to write 32 bit ints, then just slice off the last byte.
  var bits = 0
  for(var i in ary) {
    //if we don't go over an int
    if(~~((bits + width) / 32) == ~~(bits/32)) {
      
    }
  }
}

tape('bit pack into int', function (t) {
  //pack a random int, into a 32 bit int, with various bit widths.
  var n = 8, store = 0
  for(var i = 0; i  < (32 - 32%3); i+=3) {
    var r = ~~(Math.random()*8)
    console.log('toPack', r)
    store = pack(store, r, 3)
  }
  //zero fill to the end of the byte.
//  store = store << (32%2)
//
//  //uncap to reopen the store...
//  store = store >> (32%2)
//

  for(var i = 0; i < (32 - 32%3); i+=3) {
    var r = peek(store, 3)
    store = shift(store, 3)
    console.log("toUnpack", r)
  }

  console.log(bytes32(store))
  console.log('OTU', u.bufferToBits(bytes32(store)))
  t.end()
})







