const varint = require('varint')

function encodeFixedInt(value, bytes) {
  switch (bytes) {
    case 1: { let b = Buffer.alloc(1); b.writeUInt8(value); return b; }
    default: throw 'invalid argument;'
  }
}

function decodeFixedInt(buffer, offset, len) {
  switch (len) {
    case 1: { return buffer.readUInt8(offset); }
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
  for (let i = 0; i < values.length; ++i) {
    buf = Buffer.concat([buf, Buffer.from(varint.encode(1 << 1)), encodeFixedInt(values[i], Math.ceil(opts.bitWidth / 8))]);
  }

  if (opts.disableEnvelope) {
    return buf;
  }

  let envelope = Buffer.alloc(buf.length + 4);
  envelope.writeUInt32LE(buf.length);
  buf.copy(envelope, 4);

  return envelope;
}

exports.decodeValues = function(type, cursor, count, opts) {
  if (!('bitWidth' in opts)) {
    throw 'bitWidth is required';
  }

  const cursorLength = cursor.buffer.readUInt32LE(cursor.offset);
  cursor.offset += 4;
  const cursorEnd = cursor.offset + cursorLength;

  let values = [];
  while (cursor.offset < cursorEnd) {
    const header = varint.decode(cursor.buffer, cursor.offset);
    cursor.offset += varint.encodingLength(header);

    if (header & 1) {
      // bit packed run
      const valuesCount = (header >> 1) * 8;
      const values = decodeBitPacked(
          cursor.buffer,
          cursor.offset,
          valuesCount,
          opts.bitWidth);

      values.push(...values);
      cursor.offset += valuesCount * opts.bitWidth / 8;
    } else {
      // rle run
      const valueCount = header >> 1;
      const valueSize = Math.ceil(opts.bitWidth / 8);
      const value = decodeFixedInt(cursor.buffer, cursor.offset, valueSize);
      values.push(...new Array(valueCount).fill(value));
      cursor.offset += valueSize;
    }
  }

  if (values.length !== count) {
    throw "invalid RLE encoding";
  }

  return values;
}

