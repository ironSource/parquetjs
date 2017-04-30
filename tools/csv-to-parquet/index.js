var spawn = require('child_process').spawn
var path = require('path')
var os = require('os')
var mkdirp = require('mkdirp')
var create = require('./create')
var fs = require('fs')

module.exports = function (input, schema, cb) {

  //various querks around how apache-drill does directories
  var _dir = path.join('csv2parquet', ''+Date.now())
  var input_dir = path.join(os.tmpdir(), _dir, 'input')
  var input_file = path.join(input_dir, 'data.csv')
  var output_dir = path.join(_dir, 'output')
  var query_file = path.join(os.tmpdir(), _dir, 'query.sql')

  mkdirp(input_dir, function (err) {
    if(err) return cb(err)

    fs.createReadStream(input)
    .pipe(fs.createWriteStream(input_file))
    .on('finish', function () {
      var query = create(input_file, schema, output_dir)
      fs.writeFile(
        query_file,
        query,
        'utf8',
        function (err) {
          if(err) return cb(err)

          var cp = spawn(
            'java',
            [
              '-cp',
              './apache-drill-1.10.0/conf:./apache-drill-1.10.0/jars/*:./apache-drill-1.10.0/jars/ext/*:./apache-drill-1.10.0/jars/3rdparty/*:./apache-drill-1.10.0/jars/classb/*',
              'sqlline.SqlLine',
              '-d',
              'org.apache.drill.jdbc.Driver',
              '-u',
              'jdbc:drill:zk=local',
              '--run='+query_file
            ],
            {
              cwd: __dirname
            }
          )

          cp.stderr.pipe(process.stderr)

          cp.on('close', function (code) {
            if(code) cb(new Error('exit code:'+code))
            else cb(null, path.join(os.tmpdir(), output_dir))
          })
        }
      )
    })
  })
}

if(!module.parent) {
  module.exports(
    path.join(__dirname, 'example', 'plays.csv'),
    require('./example/schema.json'),
    function (err, output) {
      if(err) throw err
      console.log('OUTPUT', output)
    }
  )
}

