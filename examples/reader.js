'use strict';
const parquet = require('..');

parquet.ParquetReader.openFile('fruits.parquet', (err, reader) => {
  console.log("opened", err, reader);
  reader.close();
});

