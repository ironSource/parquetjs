var path = require('path')
//{name: type,...}

module.exports = function (input, schema, output) {
  var a = [], i = 0
  for(var k in schema)
    a.push('  cast(columns['+(i++)+'] as '+schema[k]+') `'+k + '` ')

  return (
    'ALTER SESSION SET `store.format`=\'parquet\';\n'+
    'CREATE TABLE dfs.tmp.`'+output+'/` AS\n' +
    'SELECT\n'+ a.join(',\n') + '\nFROM dfs.`'+input+'`;'
  )
}

if(!module.parent)
  console.log(module.exports(process.argv[2], {year: 'int', play: 'varchar'}, Date.now()))

