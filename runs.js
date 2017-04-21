
//check how many inputs can be rle encoded
exports.rle = function (inputs, width) {
  var value = inputs[0]
  if(inputs[1] !== inputs[0]) return null

  for(var i = 2; i < inputs.length && inputs[i] == value; i++)
    if(inputs[i] != value) {
      return {value: value, repeats: i-1}
    }
  return {
    value: value,
    repeats: i,
    width: width,
    length: Math.ceil(width/8)*i
  }
}

//hmm, bitpacking is used whenever rle can't be used.
exports.bitpacking = function (inputs, width) {

}
