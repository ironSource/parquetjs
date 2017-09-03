var julian = require('julian')
//dumped by parquet tools
var dates = [
  '46017405589295292557042944',
  '131460287298661935608767744',
  '82530434391491743162836224',
  '97339844917296871895999744',
  '165526002266680915788834048'
]

var dates_base64 = [
  'AAAoNYk8AADMgCUA',
  'AIyrKCdGAADHgCUA',
  'APBne9csAADMgCUA',
  'ABzlbN9NAADGgCUA',
  'AHBx0GAQAADGgCUA'
].map(function (e) { return new Buffer(e, 'base64') })


var dates = require('./timestamp')

var decoded = dates_base64.map(dates.decode)
console.log(decoded)
require('assert').deepEqual(decoded.map(dates.encode), dates_base64)
//console.log(dates_base64)




