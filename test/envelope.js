fs = require('fs');
parquet = require('../lib/writer.js');

var schema = new parquet.ParquetSchema();
schema.addColumn("num");

var writer = new parquet.ParquetFileWriter(schema, 'test.parquet');
writer.end();

// inspect file with
// $ hadoop jar parquet-tools-1.9.0.jar meta test.parquet
// $ hadoop jar parquet-tools-1.9.0.jar dump test.parquet
