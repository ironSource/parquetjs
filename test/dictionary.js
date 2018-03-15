'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('../parquet.js');
const path = require('path');

describe('dictionary encoding', async function() {
  it('should read uncompressed dictionary from spark', async function() {
    let reader =  await parquet.ParquetReader.openFile(path.resolve(__dirname,'test-files/spark-uncompressed-dict.parquet'));
    let cursor = reader.getCursor();
    let records = [];

    for (let i = 0; i < 5; i++) {
      records.push(await cursor.next());
    }

    assert.deepEqual(records.map(d => d.name),['apples','oranges','kiwi','banana','apples']);
  });
});