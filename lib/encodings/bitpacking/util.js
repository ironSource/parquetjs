
exports.byteToBits = function (n) {
  var s = ''
  var bit = 128
//  for(var bit = 128; bit > 0; bit =>> 1)
//    s += +!!(n & bit)

  return ('' +
    (+!!(n & 128)) +
    (+!!(n & 64)) +
    (+!!(n & 32)) +
    (+!!(n & 16)) +
    (+!!(n & 8)) +
    (+!!(n & 4)) +
    (+!!(n & 2)) +
    (n & 1)
  )

  return s
}

exports.bufferToBits = function (buffer, width) {
  var s = ''
  for(var i = 0; i < buffer.length; i++)
    s += exports.byteToBits(buffer[i]) + (i + 1 < buffer.length ? ' ' : '')

  if(width != null) {
    var rx = new RegExp('((?:[01]\\s?){'+width+'})')
    console.log(rx)
    s = s.split(rx).filter(Boolean).join(',')
  }

  return s
}

exports.bits = function (n) {
  return n ? Math.floor(Math.log(n)/Math.LN2)+1 : 0
}

