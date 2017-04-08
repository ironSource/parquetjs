
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

exports.bitsToString = function (buffer) {

}

