const rle = require('./rle');

exports.decodeValues = function(type, cursor, count, opts) {
  opts.bitWidth = cursor.buffer.slice(cursor.offset, cursor.offset+1).readInt8();
  cursor.offset += 1;
  return rle.decodeValues(type, cursor, count, Object.assign({}, opts, {disableEnvelope: true}));
};