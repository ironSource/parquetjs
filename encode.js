var Int64 = require('node-int64')

//encode an empty parquet file.
//should be like this:
/*
PAR1
<FileMetadata>
<length(FileMetaData)>
PAR1
*/

var thrift = require('thrift')
var types = require('./gen-nodejs/parquet_types')

function plain(value) {
  var v = new Buffer(value)
  var len = new Buffer(4)
  len.writeUInt32LE(v.length, 0)
  return Buffer.concat([len, v])
}

module.exports = function () {
  var PAR1 = new Buffer("PAR1")

  var five = new Int64(5)

  var fmd = new types.FileMetaData()
  var _schema = new types.SchemaElement()
  _schema.name = 'hive_schema'
  _schema.num_children = 1

  var schema = new types.SchemaElement()
  schema.name = 'id'
  schema.type = 6 //binary aka string
  schema.repetition_type = 1
  schema.converted_type = 0

//  var id_data = new Buffer('150015cc0315cc032c150a1500150615081c182933363438376235342d346563392d343330312d613037352d3939643066393237366334652e32393232182933346663323165312d356638612d346431642d616435322d6439633330653139376134312e31303933160000000002000000031f2900000033356135616231312d393666632d346463622d393233612d3834663134663232313366372e343731322900000033346663323165312d356638612d346431642d616435322d6439633330653139376134312e313039332900000033353331623236632d643762312d343362322d386261312d6361333664356131373736622e383037382800000033356630306234382d653966662d343234652d396438352d6461363262323962336136372e3138332900000033363438376235342d346563392d343330312d613037352d3939643066393237366334652e32393232', 'hex')

  var id_data =
    Buffer.concat([
      new Buffer(0),
      plain('one'),
      plain('two'),
      plain('three'),
      plain('four'),
      plain('five')
    ])

  var row_group = new types.RowGroup()
  //row group has
  // - columns
  // - total_byte_size
  // - num_rows
  // - sorting_columns

  var column = new types.ColumnChunk()
  var metadata = new types.ColumnMetaData()

  row_group.columns = [column]
  row_group.num_rows = five
  row_group.total_byte_size = new Int64(id_data.length)

  column.file_offset = new Int64(4) //starts just after the PAR1 magic number.
  column.meta_data = metadata
  metadata.type = 6
  metadata.encodings = [2, 4, 3]
  metadata.path_in_schema = ['id']
  //must set the number as a string, because parquet does not check null properly
  //and will think the value is not provided if it is falsey (includes zero)
  metadata.codec = '0'
  metadata.num_values = five
  metadata.total_uncompressed_size = new Int64(id_data.length)
  metadata.total_compressed_size = new Int64(id_data.length)
  console.error(id_data, id_data.length)
  metadata.data_page_offset = new Int64(4) //just after PAR1
  //metadata.statistics is apparently optional.

  var zero = new Buffer(8)
  zero.fill(0)
  fmd.version = 1
  fmd.schema = [_schema, schema]
  fmd.num_rows = five
  fmd.row_groups = [row_group]
//  fmd.key_value_metadata = []
  fmd.created_by = 'parquet.js@'+require('./package.json').version 

  var output = []
  var b = new Buffer(1024)
  b.fill(0)
  var transport = new thrift.TBufferedTransport(null, function (buf) {
    output.push(buf)
  })
  var protocol = new thrift.TCompactProtocol(transport)
  fmd.write(protocol)
  transport.flush()
//  protocol.flush()

  var _output = Buffer.concat(output)
  var len = new Buffer(4)
  len.writeUInt32LE(_output.length, len)

  return Buffer.concat([
    PAR1,
    id_data,
    _output,
    len,
    PAR1
  ])

}

if(!module.parent) process.stdout.write(module.exports())













