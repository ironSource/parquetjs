'use strict';
const parquet = require('..');

parquet.ParquetReader.openFile('fruits.parquet').then((reader) => {
  console.log(reader);
  reader.close();
});

