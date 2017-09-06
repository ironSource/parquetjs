'use strict';
const parquet = require('..');

parquet.ParquetReader.openFile('fruits.parquet', (err, reader) => {
  if (err) {
    console.log("ERR", err);
    process.exit(1);
  }

  console.log("opened", err, reader);
  reader.close();
});

