#! /usr/bin/env node

var fs = require('fs')

fs.stat(path.join(__dirname, 'apache-drill-1.10.0'), function (err, stat) {
  if(stat && stat.isDirectory()) return //already installed

  require('request')
    .get(''http://www.apache.org/dyn/closer.lua?filename=drill/drill-1.10.0/apache-drill-1.10.0.tar.gz&action=download')
    .pipe(require('tar-fs').extract(__dirname)
})

