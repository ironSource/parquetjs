fs = require('fs');
parquet = require('..');

var schema = new parquet.ParquetSchema();
schema.addColumn({name: "num", type: "INT64"});

var writer = new parquet.ParquetFileWriter(schema, 'test.parquet');
for (var i = 0; i < 10; ++i) {
  writer.writeRow({ "num": i })
}

writer.end();

// inspect file with
// $ hadoop jar parquet-tools-1.9.0.jar meta test.parquet
// $ hadoop jar parquet-tools-1.9.0.jar dump test.parquet
