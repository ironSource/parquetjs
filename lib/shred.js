"use strict";

function shredRecord(schema, record, data, rlvl, dlvl) {
  schema.columns.forEach(function(col_def) {
    var k = col_def.name;

    if (!(k in record)) {
      if (col_def.repetition_type == "REQUIRED") {
        throw "missing required column: " + col_def.name;
      } else {
        data[k].push({
          value: null,
          rlevel: rlvl,
          dlevel: dlvl,
        });
      }

      return;
    }

    if (typeof record[k] === Array) {
      if (col_def.repetition_type != "REPEATED") {
        throw "array requires repeated column: " + col_def.name;
      }

      record[k].forEach(function(v) {
        data[k].push({
          value: v,
          rlevel: col_def.rlvl_max,
          dlevel: col_def.dlevel_max,
        });
      });
    } else {
      data[k].push({
        value: record[k],
        rlevel: rlvl,
        dlevel: col_def.dlevel_max,
      });
    }
  });
}

function shredRecords(schema, records) {
  var cols = {};

  schema.columns.forEach(function(col_def) {
    cols[col_def.name] = [];
  });

  records.forEach(function(record) {
    shredRecord(schema, record, cols, 0, 0);
  });

  return cols;
}

module.exports = { shredRecords };

