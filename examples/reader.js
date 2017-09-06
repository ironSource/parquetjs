'use strict';
const parquet = require('..');

async function example() {
  let reader = await parquet.ParquetReader.openFile('fruits.parquet');
  console.log(reader);
  reader.close();
}

example();

