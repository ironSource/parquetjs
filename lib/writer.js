"use strict";

/**
 * Writes the envelope/outer structure of a parquet file to an output stream
 */
const PARQUET_MAGIC = "PAR1";

class ParquetEnvelopeWriter {

  constructor(write_cb) {
    this.write_cb = write_cb;
  }

  writeHeader() {
    // 4 bytes header magic 'PAR1'
    this.write_cb(Buffer.from(PARQUET_MAGIC));
  }

  writeRowGroup(row_group) {
    this.write_cb(row_group)
  }

  writeFooter(footer) {
    // footer data
    this.write_cb(footer)

    // 4 bytes footer length, uint32 LE (assumed)
    // FIXME: parquet documentation doesnt mention if this should be LE or BE, figure out and fix
    var buf = new Buffer(8);
    buf.writeUInt32LE(footer.length);

    // 4 bytes footer magic 'PAR1'
    buf.fill(PARQUET_MAGIC, 4);
    this.write_cb(buf);
  }

}

/**
 * Write a parquet file to the file system
 */
class ParquetFileWriter {

  constructor(path) {
    this.os = fs.createWriteStream('test.dat');

    var write_cb = (function(t) {
      return function(buffer) {
        t.os.write(buffer);
      };
    })(this);

    this.envelope_writer = new ParquetEnvelopeWriter(write_cb);
    this.envelope_writer.writeHeader();
  }

  end() {
    this.envelope_writer.writeFooter(Buffer.from("test"));
    this.os.end();
    this.os = null;
  }

}

module.exports = {
  ParquetFileWriter, ParquetFileWriter
}

