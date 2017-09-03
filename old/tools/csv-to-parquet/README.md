# csv-to-parquet

Convert a csv file to parquet using apache-drill.

Based on https://mapr.com/blog/how-convert-csv-file-apache-parquet-using-apache-drill/
and same idea as https://github.com/redsymbol/csv2parquet/ except you can install it
with `npm`. You'll need to have java 1.7 or 1.8 already installed on your system,
but this module will pull down `apache-drill@1.10.0` and in the post-install script.
(note: apache drill comes with all dependencies bundled, but is 180 mb)

## api

This exports a single function, which takes a csv file, a schema object, and a callback.

``` js
var toParquet = require('csv-to-parquet')

toParquet('./example/plays.csv', {
  year: 'int', play: 'varchar'
}, function (err, output_dir) {
  if(err) throw err
  //if success, there will be parquet files in output_dir
})
```

## License

MIT




