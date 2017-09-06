'use strict';
const parquet = require('..');

parquet.ParquetReader.openFile('fruits.parquet', (err, reader) => {
  if (err) {
    console.log(err);
    process.exit(1);
  }


});

