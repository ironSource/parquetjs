fs = require('fs');
parquet = require('../lib/writer.js');

var writer = new parquet.ParquetFileWriter('test.dat');
writer.end();

