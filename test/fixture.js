
var data = require('fs')
  .readFileSync(
    require('path')
    .join(__dirname, 'data.txt'),
    'utf8'
  )

module.exports = {}

data.split('\n').forEach(function (e) {
  if(e) {
    var m = /^LOG\:(\w+) ([0-9a-f]+)$/.exec(e)
   // console.log(m)
    module.exports[m[1]] = new Buffer(m[2], 'hex')
  }
})

if(!module.parent)
  console.log(module.exports)
