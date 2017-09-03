var File = require('pull-file')
var Split = require('pull-split')
var pull = require('pull-stream')
var toPull = require('stream-to-pull-stream')
var Encode = require('../encode')
var Group = require('pull-group')
var MapLast = require('pull-map-last')
var fs = require('fs')
var opts = require('minimist')(process.argv.slice(2))

var HRLE = require('../encodings/hybrid')(1)

function parseJsonLines () {
  return Split('\n', JSON.parse, null, true)
}

module.exports = Parquet

function Parquet (schema, rows) {
  var encoder = Encode(schema)

  return pull(
    Group(rows || 10000),
    MapLast(encoder, encoder)
  )
}

function filter(start, keys) {
  var o ={}
  for(var k in keys)
    o[k] = start[k]
  return o
}

if(!module.parent) {
  var schema = JSON.parse(fs.readFileSync(process.argv[3]))
  var n = 0, t = 0
  var keys = Object.keys(schema)
  pull(
    File(process.argv[2]),
    parseJsonLines(),
    Parquet(opts.filter ? filter(schema, opts.filter) : schema), //, {product:true, osversion: true})),
    toPull.sink(process.stdout)
  )
}




