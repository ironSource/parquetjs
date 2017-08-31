var INT53 = require('int53')
var parquet_thrift = require('../../gen-nodejs/parquet_types')

function encodeValues_BOOLEAN(values) {
  var buf = new Buffer(Math.ceil(values.length / 8));
  buf.fill(0);

  for(var i = 0; i < values.length; ++i) {
    if (values[i]) {
      buf[i / 8] = buf[i / 8] | (1 << (i % 8));
    }
  }

  return buf;
}

function encodeValues_INT32(values) {
  var buf = new Buffer(4 * values.length);
  for (var i = 0; i < values.length; i++) {
    buf.writeInt32LE(values[i], i * 4)
  }

  return buf;
}

function encodeValues_INT64(values) {
  var buf = new Buffer(8 * values.length);
  for (var i = 0; i < values.length; i++) {
    INT53.writeUInt64LE(values[i], buf, i * 8)
  }

  return buf;
}

//const ENCODERS = {
//  BYTE_ARRAY: function (column) {
//    return Buffer.concat([
//      //these 6 bytes are actually a hybrid RLE, it seems of the repetition level?
//      //the column starts with a hybrid-rle/bitpack of the definition
//      //level. for a flat schema with all fields, that is the
//      //same as a lot of 1's. that can be encoded most compactly
//      //as a RLE.
//
//      //Question: how is the bitwidth of the RLE calculated?
//      //I'm guessing it's something in the schema?
//      //      encodeRepeats(column.length, 1)
//    ].concat(column.map(plain)))
//
//  },
//  INT96: function (column) { //timestamps
//    var b = new Buffer(12*column.length)
//    for(var i = 0; i < column.length; i++)
//      timestamp.encode(column[i], b, i*12)
//    return b
//  },
//  FLOAT: function (column) {
//    var b = new Buffer(4*column.length)
//    for(var i = 0; i < column.length; i++)
//      b.writeFloatLE(column[i], i*4)
//    return b
//  },
//  DOUBLE: function (column) {
//    var b = new Buffer(8*column.length)
//    for(var i = 0; i < column.length; i++)
//      b.writeDoubleLE(column[i], i*8)
//    return b;
//  },
//};

exports.encodeValues = function(type, values) {
  switch (type) {

    case "BOOLEAN":
      return encodeValues_BOOLEAN(values);

    case "INT32":
      return encodeValues_INT32(values);

    case "INT64":
      return encodeValues_INT64(values);

    default:
      throw "unsupported type: " + type;

  }
}

