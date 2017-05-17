var Encode = require('../encode')

var append = Encode(
  ['a', 'b', 'c'],
  ['BYTE_ARRAY', 'INT32', "INT64"]
)

process.stdout.write(append([
  ['one',   10, Date.now()],
  ['two',   20, Date.now()+1000],
  ['three', 30, Date.now()+10000],
  ['four',  40, Date.now()+100000],
  ['five',  50, Date.now()+1000000]
]))

process.stdout.write(append([
  ['one',   10, Date.now()],
  ['two',   20, Date.now()+1000],
  ['three', 30, Date.now()+10000],
  ['four',  40, Date.now()+100000],
  ['five',  50, Date.now()+1000000]
]))

process.stdout.write(append())

