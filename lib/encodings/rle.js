var varint = require('varint')

exports.encodeValues = function(type, values, opts) {
  if (!("bit_width" in opts)) {
    throw "bit_width is required";
  }

  switch (type) {

    case "BOOLEAN":
    case "INT32":
    case "INT64":
      break;

    default:
      throw "unsupported type: " + type;

  }

  let buf = Buffer.alloc(0);
  if (opts.bit_width == 0) {
    return buf;
  }

  let run_size = 16; // FIXME make configurable
  for (let i = 0; i < values.length; i += run_size) {
    //buf = Buffer.concat(
    //    buf,
    //    encodeRun(values.slice(i, i + Math.min(values.length - i, run_size))));
  }

  return buf;
}

