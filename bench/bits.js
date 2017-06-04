
//which way is faster to get number of bits needed to express an int?

// this isn't actually a very hot path, but I got curious.

function bitsLog (n) {
  return n ? Math.floor(Math.log(n)/Math.LN2)+1 : 0
}
function bitsBitwise (n) {
  var i = 0
  while(n) {
    n >>= 1
    i++
  }
  return i
}

function bitsUnrolled (n) {
  var i = 0
  if(n===0) return 0
  else if(n>> 1 === 0) return  1
  else if(n>> 2 === 0) return  2
  else if(n>> 3 === 0) return  3
  else if(n>> 4 === 0) return  4
  else if(n>> 5 === 0) return  5
  else if(n>> 6 === 0) return  6
  else if(n>> 7 === 0) return  7
  else if(n>> 8 === 0) return  8
  else if(n>> 9 === 0) return  9
  else if(n>>10 === 0) return 10
  else if(n>>11 === 0) return 11
  else if(n>>12 === 0) return 12
  else if(n>>13 === 0) return 13
  else if(n>>14 === 0) return 14
  else if(n>>15 === 0) return 15
  else if(n>>16 === 0) return 16
  else if(n>>17 === 0) return 17
  else if(n>>18 === 0) return 18
  else if(n>>19 === 0) return 19
  else if(n>>20 === 0) return 20
  else if(n>>21 === 0) return 21
  else if(n>>22 === 0) return 22
  else if(n>>23 === 0) return 23
  else if(n>>24 === 0) return 24
  else if(n>>25 === 0) return 25
  else if(n>>26 === 0) return 26
  else if(n>>27 === 0) return 27
  else if(n>>28 === 0) return 28
  else if(n>>29 === 0) return 29
  else if(n>>30 === 0) return 30
  else if(n>>31 === 0) return 31
  else if(n>>32 === 0) return 32
}

var start = Date.now()
var l = 10000000
while(l--) {
  for(var i = 0; i < 10; i++)
    bitsLog(l)
}
console.log(Date.now() - start)

var start2 = Date.now()
var l = 10000000
while(l--) {
  for(var i = 0; i < 10; i++)
    bitsBitwise(l)
}
console.log(Date.now() - start2)

var start3 = Date.now()
var l = 10000000
while(l--) {
  for(var i = 0; i < 10; i++)
    bitsUnrolled(l)
}
console.log(Date.now() - start3)



