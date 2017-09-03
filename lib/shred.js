"use strict";

function shredRecords(schema, records) {
  var cols = {};
  var cols_def = {};

  schema.columns.forEach(function(col_def) {
    cols[col_def.name] = [];
    cols_def[col_def.name] = col_def;
  });

  records.forEach(function(record) {
    Object.keys(cols).forEach(function(k) {
      cols[k].push({
        value: k in record ? record[k] : null,
        rlevel: 0,
        dlevel: k in record ? cols_def[k].dlevel_max : 0,
      });
    });
  });

  return cols;
}

module.exports = { shredRecords };

