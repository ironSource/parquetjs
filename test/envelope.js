var parquet = require('..');

// create a schema with a single, required int64 column 'num'
var schema = new parquet.ParquetSchema();
schema.addColumn({
  name: "num",
  repetition_type: "REQUIRED",
  type: "INT64",
  encoding: "PLAIN"
});

// create a parquet writer
var writer = new parquet.ParquetFileWriter(schema, 'test.parquet');

// write 10 rows to the parquet file with num=[0..10)
for (var i = 0; i < 10; ++i) {
  writer.writeRow({ "num": i })
}

writer.end();

// inspect the output file with
// $ hadoop jar parquet-tools-1.9.0.jar meta test.parquet
// $ hadoop jar parquet-tools-1.9.0.jar dump test.parquet
