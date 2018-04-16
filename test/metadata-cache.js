'use strict';
const chai = require('chai');
const assert = chai.assert;
const path = require('path');
const parquet = require('../parquet.js');

describe('metadata-cache', function() {  
  let metadata;

  before(async function() {
    const reader = await parquet.ParquetReader.openFile(path.join(__dirname,'test-files','fruits.parquet'));   
    for (let i = 0; i < reader.metadata.row_groups.length; i++) {
      const rowGroup = reader.metadata.row_groups[i];
      for (let j = 0; j < rowGroup.columns.length; j++) {
        const column = rowGroup.columns[j];
        try {
          await reader.envelopeReader.readOffsetIndex(column.meta_data.path_in_schema.join(','), rowGroup, {cache: true});
          await reader.envelopeReader.readColumnIndex(column.meta_data.path_in_schema.join(','), rowGroup, {cache: true});
          column.offset_index_offset = undefined;
          column.offset_index_length = undefined;
          column.column_index_offset = undefined;
          column.column_index_length = undefined;
        } catch(e) {}
      }
    }
    const metaDataTxt = reader.exportMetadata();
    metadata = JSON.parse(metaDataTxt);
  });

  it('should work', async function() {
    const reader = await parquet.ParquetReader.openFile(path.join(__dirname,'test-files','fruits.parquet'),{
      metadata: metadata
    });
    const column = reader.metadata.row_groups[0].columns[2];
    
    // verify that the json metadata is loaded
    assert.equal(reader.metadata.json,true);

    const data = await reader.envelopeReader.readPage(column, 1, []);
    assert.equal(data.length,2000);
    assert.deepEqual(data[0],{price: 2.6});
    assert.deepEqual(data[1],{price: 2.7});
    assert.deepEqual(data[2],{price: 4.2});
  });

});
