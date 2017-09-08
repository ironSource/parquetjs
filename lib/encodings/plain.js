const INT53 = require('int53')
const parquet_thrift = require('../../gen-nodejs/parquet_types')

function encodeValues_BOOLEAN(values) {
  let buf = new Buffer(Math.ceil(values.length / 8));
  buf.fill(0);

  for (let i = 0; i < values.length; ++i) {
    if (values[i]) {
      buf[Math.floor(i / 8)] |= (1 << (i % 8));
    }
  }

  return buf;
}

function encodeValues_INT32(values) {
  let buf = new Buffer(4 * values.length);
  for (let i = 0; i < values.length; i++) {
    buf.writeInt32LE(values[i], i * 4)
  }

  return buf;
}

function encodeValues_INT64(values) {
  let buf = new Buffer(8 * values.length);
  for (let i = 0; i < values.length; i++) {
    INT53.writeInt64LE(values[i], buf, i * 8);
  }

  return buf;
}

function encodeValues_INT96(values) {
  let buf = new Buffer(12 * values.length);

  for (let i = 0; i < values.length; i++) {
    if (values[i] >= 0) {
      INT53.writeInt64LE(values[i], buf, i * 12);
      buf.writeUInt32LE(0, i * 12 + 8); // js doesnt have >64bit ints
    } else {
      INT53.writeUInt64LE((~-values[i]) + 1, buf, i * 12);
      buf.writeUInt32LE(0xffffffff, i * 12 + 8); // js doesnt have >64bit ints
    }
  }

  return buf;
}

function encodeValues_FLOAT(values) {
  let buf = new Buffer(4 * values.length);
  for (let i = 0; i < values.length; i++) {
    buf.writeFloatLE(values[i], i * 4)
  }

  return buf;
}

function encodeValues_DOUBLE(values) {
  let buf = new Buffer(8 * values.length);
  for (let i = 0; i < values.length; i++) {
    buf.writeDoubleLE(values[i], i * 8)
  }

  return buf;
}

function encodeValues_BYTE_ARRAY(values) {
  let buf_len = 0;
  for (let i = 0; i < values.length; i++) {
    values[i] = Buffer.from(values[i]);
    buf_len += 4 + values[i].length;
  }

  let buf = Buffer.alloc(buf_len);
  let buf_pos = 0;
  for (let i = 0; i < values.length; i++) {
    buf.writeUInt32LE(values[i].length, buf_pos)
    values[i].copy(buf, buf_pos + 4);
    buf_pos += 4 + values[i].length;

  }

  return buf;
}

exports.encodeValues = function(type, values) {
  switch (type) {

    case 'BOOLEAN':
      return encodeValues_BOOLEAN(values);

    case 'INT32':
      return encodeValues_INT32(values);

    case 'INT64':
      return encodeValues_INT64(values);

    case 'INT96':
      return encodeValues_INT96(values);

    case 'DOUBLE':
      return encodeValues_DOUBLE(values);

    case 'FLOAT':
      return encodeValues_FLOAT(values);

    case 'BYTE_ARRAY':
      return encodeValues_BYTE_ARRAY(values);

    default:
      throw 'unsupported type: ' + type;

  }
}

exports.decodeValues = function(type, buffer, offset, count, output, opts) {
  switch (type) {

    default:
      throw 'unsupported type: ' + type;

  }
}

