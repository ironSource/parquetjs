var Int64 = require('node-int64')
var Int53 = require('int53')
var varint = require('varint')
var explain = require('explain-error')
var timestamp = require('./timestamp')
var HRLE = require('./encodings/hybrid')(1)


//encode an empty parquet file.
//should be like this:

/*
PAR1
<FileMetadata>
<length(FileMetaData)>
PAR1
*/

var convertedType = {
  string: 'UTF8'
}
var encodingType = {
  string: 'BYTE_ARRAY',
  int: 'INT32',
  double: 'DOUBLE',
  timestamp: 'INT96',
  boolean: 'BOOLEAN',
  smallint: 'INT32'
}

var zeros = new Buffer(12)
zeros.fill(0)

var defaults = {
  "BYTE_ARRAY": "",
  INT32: 0,
  INT64: 0,
  INT96: zeros,
  DOUBLE: 0,
  float: 0,
  boolean: false,
}

var BufferList = require('bl')

var thrift = require('thrift')
var pt = require('./gen-nodejs/parquet_types')

function plain(value) {
//  if(value == null) throw new Error('falsy value:'+JSON.stringify(value))
  var v = new Buffer(value || defaults.BYTE_ARRAY) 
  var len = new Buffer(4)
  len.writeUInt32LE(v.length, 0)
  return Buffer.concat([len, v])
}

//value must be a thrift type.
function encode(value) {
  var output = []
  var transport = new thrift.TBufferedTransport(null, function (buf) {
    output.push(buf)
  })
  var protocol = new thrift.TCompactProtocol(transport)
  value.write(protocol)
  transport.flush()
  return Buffer.concat(output)
}

function encodeRepeats(repeats, value) {
  var len = varint.encodingLength(repeats << 1)
  var b = new Buffer(4 + len + 1)
  b.writeUInt32LE(len+1, 0)
  varint.encode(repeats << 1, b, 4)
  b[4 + len] = value
  return b
}

function encodeNulls(nulls) {
  var b = new Buffer(1024)
  HRLE.encode(nulls, b, 4)
  b.writeUInt32LE(HRLE.encode.bytes, 0)
  return b.slice(0, HRLE.encode.bytes+4)
}


var encodeValues = {
  BYTE_ARRAY: function (column) {
    return Buffer.concat([
      //these 6 bytes are actually a hybrid RLE, it seems of the repetition level?
      //the column starts with a hybrid-rle/bitpack of the definition
      //level. for a flat schema with all fields, that is the
      //same as a lot of 1's. that can be encoded most compactly
      //as a RLE.

      //Question: how is the bitwidth of the RLE calculated?
      //I'm guessing it's something in the schema?
      //      encodeRepeats(column.length, 1)
    ].concat(column.map(plain)))

  },
  INT32: function (column) {
    var b = new Buffer(4*column.length)
    for(var i = 0; i < column.length; i++)
      b.writeInt32LE(column[i] || defaults.INT32, i*4)
    return b
  },
  INT64: function (column) {
    var b = new Buffer(8*column.length)
    for(var i = 0; i < column.length; i++)
      Int53.writeUInt64LE(column[i] || defaults.INT64, b, i*8)
    return b
  },
  INT96: function (column) { //timestamps
    var b = new Buffer(12*column.length)
    for(var i = 0; i < column.length; i++)
      timestamp.encode(column[i] || defaults.INT96, b, i*12)
    return b
  },
  FLOAT: function (column) {
    var b = new Buffer(4*column.length)
    for(var i = 0; i < column.length; i++)
      b.writeFloatLE(column[i] || defaults.FLOAT, i*4)
    return b
  },
  DOUBLE: function (column) {
    var b = new Buffer(8*column.length)
    for(var i = 0; i < column.length; i++)
      b.writeDoubleLE(column[i] || defaults.DOUBLE, i*8)
    return b
  },
  BOOLEAN: function (column) {
    //packed into single bits.
    var b = new Buffer(Math.ceil(column.length/8))
    for(var i = 0; i < column; i+=8) {
      var byte = 0
      for(var j = 0; j < 8; j++)
        byte <<= 1 | +(column[j+i]||defaults.BOOLEAN)
      b[i/8] = byte
    }
    return b
  }
}

function encodeColumn(name, type, column) {
  if(!encodeValues[type])
    throw new Error('no value encoding:'+type)

  var data = Buffer.concat([
    encodeRepeats(column.length, 1),
    encodeValues[type](column)
  ])

  var ph = new pt.PageHeader()

  ph.type = '0' //plain encoding
  ph.uncompressed_page_size = data.length
  ph.compressed_page_size = data.length
  ph.crc = null
  ph.data_page_header = new pt.DataPageHeader()
  ph.data_page_header.num_values = (column.length).toString()
  ph.data_page_header.encoding = '0'   //plain encoding
  ph.data_page_header.definition_level_encoding = 3 //3 //RLE encoding
  ph.data_page_header.repetition_level_encoding = 4 //Bitpacked encoding
  //statistics is optional, but leaving it off probably slows
  //some queries.
  //ph.data_page_header.statistics

  var data_page = Buffer.concat([
    //unfortunately, the page header
    //is expected before the values
    //which means we can't stream the values
    //then write the header...
    //but I guess the idea is to write a column_chunk at a time
    //(with a page_header at the top)
    encode(ph),
    data
  ])

  return data_page
}

function encodeColumnV2(name, type, column) {
  if(!encodeValues[type])
    throw new Error('no value encoding:'+type)
  //column = [column.shift()]
  var values = []
  var nulls = new Array(column.length)
  column.forEach(function (e, j) {
    if(nulls[j] = +(!(e === undefined || e ===  '')))
      values.push(e)
  })

//  console.error(encodeValues[type](values))
  var dl = encodeNulls(nulls).slice(4)
  var data = Buffer.concat([
    dl,
    encodeValues[type](values)
  //: encodeValues[type](column).slice(0, 1)
//    values.length ? encodeValues[type](values) : encodeValues[type](column).slice(0, 1)
//    values.length ? encodeValues[type](values) : new Buffer(5)
  ])

  var ph = new pt.PageHeader()

  ph.type = '0' //plain encoding
  ph.uncompressed_page_size = data.length
  ph.compressed_page_size = data.length
  ph.crc = null
  ph.data_page_header_v2 = new pt.DataPageHeaderV2()
  ph.data_page_header_v2.num_values = (column.length).toString()
  ph.data_page_header_v2.num_nulls = (values.length - column.length).toString()
  ph.data_page_header_v2.num_rows = '1' //not sure what this is about, actually.
  ph.data_page_header_v2.encoding = '0'   //plain encoding
  ph.data_page_header_v2.definition_levels_byte_length = '0' //dl.length.toString()
  ph.data_page_header_v2.repetition_levels_byte_length = dl.length
//  ph.data_page_header_v2.definition_level_encoding = 3 //3 //RLE encoding
//  ph.data_page_header_v2.repetition_level_encoding = 4 //Bitpacked encoding
  //statistics is optional, but leaving it off probably slows
  //some queries.
  //ph.data_page_header.statistics

  var data_page = Buffer.concat([
    //unfortunately, the page header
    //is expected before the values
    //which means we can't stream the values
    //then write the header...
    //but I guess the idea is to write a column_chunk at a time
    //(with a page_header at the top)
    encode(ph),
    data
  ])

  return data_page
}


module.exports = function (headers, types) {

  var PAR1 = new Buffer("PAR1")
  var offset = 4 //start at 4 because of "PAR1" magic number.
  var count = 0 //table.length

  var fmd = new pt.FileMetaData()
  var _schema = new pt.SchemaElement()
  _schema.name = 'hive_schema'
  _schema.num_children = headers.length

  var schemas = headers.map(function (name, i) {
    var schema = new pt.SchemaElement()
    schema.name = name
    //note, javascript code generated by thrift does not check
    //falsey values correctly, but parquet uses an old version of thrift
    //so it's easier to set it like this.

    schema.type = ''+pt.Type[encodingType[types[i]]]
    //make every field optional
    schema.repetition_type = 1
    //schema.repetition_type = '0'

    if(convertedType[types[i]])
      schema.converted_type = ''+pt.Type[convertedType[types[i]]]

    return schema
  })

  fmd.version = 1
  fmd.schema = [_schema].concat(schemas)
  fmd.num_rows = count
  fmd.row_groups = []
  fmd.created_by = 'parquet.js@'+require('./package.json').version 

  //append a whole row_group
  return function (table) {
    if(!table) { //append the end.
      var _output = encode(fmd)
      var len = new Buffer(4)
      len.writeUInt32LE(_output.length, len)
      return Buffer.concat([_output, len, PAR1])

    } else {
     fmd.num_rows = (count += table.length)

     var columns = [] //rotate table
      table.forEach(function (row) {
        row.forEach(function (value, i) {
          columns[i] = columns[i] || []
          columns[i].push(value)
        })
      })

      var pages = [], size = 0

      var column_chunks = headers.map(function (name, i) {
        if(!encodingType[types[i]])
          throw new Error('no encoding type for:'+types[i])
        var data_page

        try {
          data_page = encodeColumn(name, encodingType[types[i]], columns[i])
        } catch(err) {
          throw explain(err, 'trying to encode column:'+name)
        }
        var column = new pt.ColumnChunk()
        var metadata = new pt.ColumnMetaData()

        column.file_offset = new Int64(offset)
        column.meta_data = metadata
        var start = offset
        pages.push(data_page)

        var type = encodingType[types[i]]
        metadata.type = ''+pt.Type[type] //XXX
        metadata.encodings = type == 'BYTE_ARRAY' ? [2, 4, 3] : [3, 4, 2]
        metadata.path_in_schema = [name]
        // must set the number as a string,
        // because parquet does not check null properly
        // and will think the value is not provided if
        // it is falsey (includes zero)

        metadata.codec = '0'
        metadata.num_values = table.length
        metadata.total_uncompressed_size = new Int64(data_page.length)
        metadata.total_compressed_size = new Int64(data_page.length)
        metadata.data_page_offset = new Int64(offset) //just after PAR1

        size += data_page.length
        offset += data_page.length

        return column
      })

      //the name "row group" suggests that a row group
      //should contain a column chunk for every row.
      //basically, we stream the input out chunks, a row group at a time.
      //these can be streamed to a file... we just save the file metadata
      //to be written at the end.

      var row_group = new pt.RowGroup()
      //row group has
      // - columns
      // - total_byte_size
      // - num_rows
      // - sorting_columns

      // with multiple columns, these will be one after another obviously.
      // for the first data_page, file_offset will be 4.
      // starts just after the "PAR1" magic number.

      row_group.columns = column_chunks
      row_group.num_rows = table.length
      row_group.total_byte_size = new Int64(size)
      fmd.row_groups.push(row_group)

      //the first item
      if(fmd.row_groups.length === 1) pages.unshift(PAR1)

      return Buffer.concat(pages)
    }
  }
}


