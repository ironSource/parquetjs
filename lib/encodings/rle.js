const varint = require('varint')

function encodeFixedInt(value, bytes) {
  switch (bytes) {
    case 1: { let b = Buffer.alloc(1); b.writeUInt8(value); return b; }
    default: throw 'invalid argument;'
  }
}

exports.encodeValues = function(type, values, opts) {
  if (!('bitWidth' in opts)) {
    throw 'bitWidth is required';
  }

  switch (type) {

    case 'BOOLEAN':
    case 'INT32':
    case 'INT64':
      values = values.map((x) => parseInt(x, 10));
      break;

    default:
      throw 'unsupported type: ' + type;
  }

  let buf = Buffer.alloc(0);
  if (opts.bitWidth == 0) {
    return buf;
  }

  for (let i = 0; i < values.length; ++i) {
    buf = Buffer.concat([buf, Buffer.from(varint.encode(1 << 1)), encodeFixedInt(values[i], Math.ceil(opts.bitWidth / 8))]);
  }

  let envelope = Buffer.alloc(buf.length + 4);
  envelope.writeUInt32LE(buf.length);
  buf.copy(envelope, 4);

  return envelope;
}

exports.decodeValues = function(type, buffer, offset, count, output, opts) {
  if (!('bitWidth' in opts)) {
    throw 'bitWidth is required';
  }

  if (opts.bitWidth == 0) {
    for (let i = 0; i < count; ++i) {
      output.push(0);
    }

    return 0;
  }

  throw "not yet implemented";
}

