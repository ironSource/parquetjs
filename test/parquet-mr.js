'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('../parquet.js');
const child_process = require('child_process');

// helper function that runs parquet-tools dump inside a docker container and returns the stdout
async function readParquetMr(file) {
  return new Promise( (resolve, reject) => {
    const dockerCmd = `docker run -v \${PWD}:/home nathanhowell/parquet-tools dump --debug /home/${file}`;
    child_process.exec(dockerCmd, (err, stdout, stderr) => {
      if (err || stderr) {
        reject(err || stderr);
      } else {
        resolve(stdout);
      }
    });
  });
}

describe('Parquet-mr', function() {
  it('should read a simple parquetjs file', async function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      quantity: { type: 'INT64' },
      price: { type: 'DOUBLE' },
    });

    const rows = [
      { name: 'apples', quantity: 10, price: 2.6 },
      { name: 'oranges', quantity: 20, price: 2.7},
      { name: 'kiwi', price: 4.2, quantity: 4},
    ];

    let writer = await parquet.ParquetWriter.openFile(schema, 'test-mr.parquet');
    
    for (let row of rows) {
      await writer.appendRow(row);
    }

    await writer.close();

    const result = await readParquetMr('test-mr.parquet');
    assert.equal(result,'row group 0 \n--------------------------------------------------------------------------------\nname:      BINARY UNCOMPRESSED DO:0 FPO:4 SZ:51/51/1.00 VC:3 ENC:PLAIN,RLE\nquantity:  INT64 UNCOMPRESSED DO:0 FPO:79 SZ:46/46/1.00 VC:3 ENC:PLAIN,RLE\nprice:     DOUBLE UNCOMPRESSED DO:0 FPO:154 SZ:46/46/1.00 VC:3 ENC:PLAIN,RLE\n\n    name TV=3 RL=0 DL=0\n    ----------------------------------------------------------------------------\n    page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:29 VC:3\n\n    quantity TV=3 RL=0 DL=0\n    ----------------------------------------------------------------------------\n    page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:24 VC:3\n\n    price TV=3 RL=0 DL=0\n    ----------------------------------------------------------------------------\n    page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:24 VC:3\n\nBINARY name \n--------------------------------------------------------------------------------\n*** row group 1 of 1, values 1 to 3 *** \nvalue 1: R:0 D:0 V:apples\nvalue 2: R:0 D:0 V:oranges\nvalue 3: R:0 D:0 V:kiwi\n\nINT64 quantity \n--------------------------------------------------------------------------------\n*** row group 1 of 1, values 1 to 3 *** \nvalue 1: R:0 D:0 V:10\nvalue 2: R:0 D:0 V:20\nvalue 3: R:0 D:0 V:4\n\nDOUBLE price \n--------------------------------------------------------------------------------\n*** row group 1 of 1, values 1 to 3 *** \nvalue 1: R:0 D:0 V:2.6\nvalue 2: R:0 D:0 V:2.7\nvalue 3: R:0 D:0 V:4.2\n');
  });

  it('should read a nested field', async function() {
    var schema = new parquet.ParquetSchema({
      fruit: {
        fields: {
          name: { type: 'UTF8'},
          quantity: { type: 'INT32'}
        }
      }
    });

    let writer = await parquet.ParquetWriter.openFile(schema, 'test2-mr.parquet');

    await writer.appendRow({
      fruit: {
        name: 'apple',
        quantity: 9
      }
    });

    await writer.close();

    const result = await readParquetMr('test2-mr.parquet');
    assert.equal(result,'row group 0 \n--------------------------------------------------------------------------------\nfruit:     \n.name:      BINARY UNCOMPRESSED DO:0 FPO:4 SZ:31/31/1.00 VC:1 ENC:PLAIN,RLE\n.quantity:  INT32 UNCOMPRESSED DO:0 FPO:65 SZ:26/26/1.00 VC:1 ENC:PLAIN,RLE\n\n    fruit.name TV=1 RL=0 DL=0\n    ----------------------------------------------------------------------------\n    page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:9 VC:1\n\n    fruit.quantity TV=1 RL=0 DL=0\n    ----------------------------------------------------------------------------\n    page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:4 VC:1\n\nBINARY fruit.name \n--------------------------------------------------------------------------------\n*** row group 1 of 1, values 1 to 1 *** \nvalue 1: R:0 D:0 V:apple\n\nINT32 fruit.quantity \n--------------------------------------------------------------------------------\n*** row group 1 of 1, values 1 to 1 *** \nvalue 1: R:0 D:0 V:9\n');
  });

  it('should read a parquetjs file with optional value', async function() {
    var schema = new parquet.ParquetSchema({
      name: { type: 'UTF8', optional: true }
    });

    const rows = [
      { name: 'apples' },
      { name: 'oranges' },
      { name: 'kiwi' },
    ];

    let writer = await parquet.ParquetWriter.openFile(schema, 'test3-mr.parquet');
     for (let row of rows) {
      await writer.appendRow(row);
    }

    await writer.close();

    const result = await readParquetMr('test3-mr.parquet');
    assert.equal(result,'row group 0 \n--------------------------------------------------------------------------------\nname:  BINARY UNCOMPRESSED DO:0 FPO:4 SZ:53/53/1.00 VC:3 ENC:PLAIN,RLE\n\n    name TV=3 RL=0 DL=1\n    ----------------------------------------------------------------------------\n    page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:31 VC:3\n\nBINARY name \n--------------------------------------------------------------------------------\n*** row group 1 of 1, values 1 to 3 *** \nvalue 1: R:0 D:1 V:apples\nvalue 2: R:0 D:1 V:oranges\nvalue 3: R:0 D:1 V:kiwi\n');
  });

  it('should read repeated fields', async function() {
    const schema =  new parquet.ParquetSchema({
      stock: {
        repeated: true,
        fields: {    
          warehouse: { type: 'UTF8' },
        }
      }
    });

    let writer = await parquet.ParquetWriter.openFile(schema, 'test4-mr.parquet');

    await writer.appendRow({
      stock: [
        {warehouse: 'Newark'}
      ]
    });

    await writer.close();
    
    const result = await readParquetMr('test4-mr.parquet');
    assert.equal(result,'row group 0 \n--------------------------------------------------------------------------------\nstock:      \n.warehouse:  BINARY UNCOMPRESSED DO:0 FPO:4 SZ:36/36/1.00 VC:1 ENC:PLAIN,RLE\n\n    stock.warehouse TV=1 RL=1 DL=1\n    ----------------------------------------------------------------------------\n    page 0:  DLE:RLE RLE:RLE VLE:PLAIN ST:[no stats for this column] SZ:14 VC:1\n\nBINARY stock.warehouse \n--------------------------------------------------------------------------------\n*** row group 1 of 1, values 1 to 1 *** \nvalue 1: R:0 D:1 V:Newark\n')
  });
});
