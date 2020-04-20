'use strict';
const chai = require('chai');
const fs = require('fs');
const assert = chai.assert;
const path = require('path');
const parquet = require('../parquet.js');
const {promisify} = require('util');

describe('test-files', function() {  
  let csv;

  async function readData(file, count) {
    let records = [],record;
    let i = 0;
    const reader = await parquet.ParquetReader.openFile(path.join(__dirname,'test-files',file));
    const cursor = reader.getCursor();
    while ( (record = await cursor.next()) && (!count ||  i++ < count)) {
      records.push(record);
    }
    return records;
  }

  function bufferToString(d) {
    if (d instanceof Buffer) {
      return d.toString();
    } else if (typeof d === 'object'){
      Object.keys(d).forEach(key => {
        d[key] = bufferToString(d[key], key);
      });
      return d;
    } else {
      return d;
    }
  }

  async function check(records, fields) {
    if (typeof records === 'string') {
      records = await readData(records);
    }
    
    records = bufferToString(records);

    assert.deepEqual(records, csv.map(d => d.reduce( (p,d,i) => {
      p[fields[i]] = isNaN(d) ? d : +d;
      return p;
    },{})));
  }

  before(async function() {
    csv = await promisify(fs.readFile)(path.join(__dirname,'test-files','nation.csv'));
    csv = csv.toString().split('\n').filter(d => d.length).map(d => d.split('|'));
  });

  it('customer.impala.parquet loads', async function() {
    this.timeout(5000);
    let data = await readData('customer.impala.parquet',100);
    bufferToString(data);
    const expected = require(path.join(__dirname,'test-files','customer.impala.json')).map(el => { return { ...el, c_custkey: BigInt(el.c_custkey)}});

    
    assert.deepEqual(data,expected);
  });

  it('gzip-nation.impala.parquet loads', async function() {    
    await check('gzip-nation.impala.parquet',['n_nationkey','n_name','n_regionkey','n_comment']);
  });

  // repeated values
  // it('nation.dict.parquet loads', async function() {
  //   await check('nation.dict.parquet',['nation_key','name','region_key','comment_col']);  
  // });
  
  it('nation.impala.parquet loads', async function() {
    await check('nation.impala.parquet', ['n_nationkey','n_name','n_regionkey','n_comment']);
  });

  it('nation.plain.parquet loads', async function() {
    let records = await readData('nation.plain.parquet');
    await check(records,['nation_key','name','region_key','comment_col']);
  });

  it('snappy-nation.impala.parquet loads', async function() {
    await check('snappy-nation.impala.parquet', ['n_nationkey','n_name','n_regionkey','n_comment']);
  });

  it('mr_times.parq loads', async function() {
    const data = await readData('mr_times.parq');
    assert.deepEqual(data,[
      {'id':'1','date_added':'83281000000000'},
      {'id':'2','date_added':'83282000000000'},
      {'id':'3','date_added':'83283000000000'},
      {'id':'4','date_added':'83284000000000'},
      {'id':'5','date_added':'83284000000000'},
      {'id':'6','date_added':'83285000000000'},
      {'id':'7','date_added':'83286000000000'},
      {'id':'8','date_added':'83287000000000'},
      {'id':'9','date_added':'83288000000000'},
      {'id':'10','date_added':'83289000000000'}
    ]);
  });

  it('nested.parq loads', async function() {
    const data = await readData('nested.parq');
    assert.deepEqual(data,[...Array(10)].map( () => ({'nest':{'thing':{'list':[{'element':'hi'},{'element':'world'}]}}})))
  });

  it('test-converted-type-null.parquet loads', async function() {
    const data = await readData('test-converted-type-null.parquet');
    assert.deepEqual(data,[{foo: 'bar'},{}]);
  });

  it('test-enum-type.parquet loads', async function() {
    const data = await readData('test-enum-type.parquet');
    assert.deepEqual(data,[{foo: 'bar'}]);
  });

  it('test-null-dictionary.parquet loads', async function() {
    const data = await readData('test-null-dictionary.parquet');
    assert.deepEqual(data,[].concat.apply([{}],[...Array(3)].map( () => ([{foo: 'bar'}, {foo: 'baz'}]))));
  });

  it('test-null.parquet loads', async function() {
    const data = await readData('test-null.parquet');
    assert.deepEqual(data,[{foo: 1, bar: 2},{foo: 1}]);
  });

  it('test.parquet loads', async function() {
    const data = await readData('test.parquet');
    bufferToString(data);
    assert.deepEqual(data.slice(0,5), [
      {'bhello':'hello','f':0,'i32':0,'i64':0n,'hello':'hello'},
      {'bhello':'people','f':1,'i32':1,'i64':1n,'hello':'people'},
      {'bhello':'people','f':2,'i32':2,'i64':2n,'hello':'people'},
      {'bhello':'people','f':3,'i32':3,'i64':3n,'hello':'people'},
      {'bhello':'you','f':4,'i32':4,'i64':4n,'hello':'you'}
    ]);
  });
});
