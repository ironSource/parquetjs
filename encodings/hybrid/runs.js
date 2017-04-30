function append(ary, item) {
  ary.push(item)
  return ary
}

function assertNaNaN (n) {
  if(isNaN(n)) throw new Error('expected not not a number')
  return n
}

module.exports = function (width) {
  var runs = []
  var buffer = []
  var previous = null
  var repeats = 1
  function appendRLE () {
    buffer = []
    runs.push({value: previous, repeats: repeats})
  }

  function appendBitpack () {
    var last = runs.length - 1
    if(Array.isArray(runs[last])) {
      runs[last] = runs[last].concat(buffer)
      if(runs[last].length > 504) {
        runs.push(runs[last].slice(504))
        runs[last] = runs[last].slice(0, 504)
      }
    }
    else
      runs.push(buffer)
    repeats = 0
    buffer = []
  }

  return {
    write: function (value) {
      if(value === previous) {
        repeats ++
        //do not add to buffer, because we will RLE
        if(repeats >= 8) return
      }
      else {
        if(repeats >= 8) {
          appendRLE()
        }
        repeats = 1
        previous = value
      }
      buffer.push(value)
      if(buffer.length == 8)
        appendBitpack()
    },
    end: function () {
      if(repeats >= 8) {
        appendRLE()
      }
      else if(buffer.length > 0) {
        while(buffer.length < 8)
          buffer.push(0)
        appendBitpack()
      }
      return runs
    }
  }
}

