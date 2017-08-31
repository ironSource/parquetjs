"use strict";

function shredRecords(schema, records) {
  var cols = {};

  schema.columns.forEach(function(col_def) {
    cols[col_def.name] = [];
  });

  records.forEach(function(record) {
    Object.keys(cols).forEach(function(k) {
      cols[k].push({
        value: record[k],
        rlevel: 0,
        dlevel: 0
      });
    });
  });

  return cols;
}

module.exports = { shredRecords };

