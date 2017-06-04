
var strings = []
var l = 100000, M = 1024
var b = require('crypto').randomBytes(1024).toString('base64')
while(l--)
  strings.push(b.substring(0, ~~(Math.random()*1024)))

function plain(value) {
  var v = new Buffer(value || '')
  var len = new Buffer(4)
  len.writeUInt32LE(v.length, 0)
  return Buffer.concat([len, v])
}

function plain1 (ary) {
  var length = 0
  for(var i = 0; i < ary.length; i++)
    length += Buffer.byteLength(ary[i]) + 4
  var buf = new Buffer(length)
  var offset = 0, i = 0
  for(var i = 0; i < ary.length; i++) {
    var len = Buffer.byteLength(ary[i])
    buf.writeUInt32LE(len, offset)
    buf.write(ary[i], offset+=4)
    offset += len
  }
  return buf
}

function plain2 (ary) {
  var length = 0, lengths = []
  for(var i = 0; i < ary.length; i++) {
    var _length = Buffer.byteLength(ary[i])
    lengths.push(_length)
    length += _length + 4
  }
  var buf = new Buffer(length)
  var offset = 0, i = 0
  for(var i = 0; i < ary.length; i++) {
    buf.writeUInt32LE(lengths[i], offset)
    buf.write(ary[i], offset+=4)
    offset += lengths[i]
  }
  return buf
}


var start = Date.now()
  plain1(strings)
console.log(Date.now() - start)

var start2 = Date.now()
  Buffer.concat(strings.map(plain))
console.log(Date.now() - start2)

var start3 = Date.now()
  plain2(strings)
console.log(Date.now() - start3)

