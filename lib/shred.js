"use strict";

/**
 * 'Shred' a record into a list of <value, repetition_level, definition_level>
 * tuples per column using the Google Dremel Algorithm.
 */
exports.shredRecords = function(schema, records) {
  let cols = {};

  schema.columns.forEach(function(col_def) {
    cols[col_def.name] = [];
  });

  records.forEach(function(record) {
    shredRecord(schema, record, cols, 0, 0);
  });

  return cols;
}

function shredRecord(schema, record, data, rlvl, dlvl) {
  schema.columns.forEach(function(col_def) {
    let k = col_def.name;

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

    if (typeof(record[k]) == "object") {
      if (col_def.repetition_type != "REPEATED") {
        throw "array requires repeated column: " + col_def.name;
      }

      for (let i = 0; i < record[k].length; ++i) {
        data[k].push({
          value: record[k][i],
          rlevel: i == 0 ? rlvl : col_def.rlevel_max,
          dlevel: col_def.dlevel_max,
        });
      }
    } else {
      data[k].push({
        value: record[k],
        rlevel: rlvl,
        dlevel: col_def.dlevel_max,
      });
    }
  });
}

