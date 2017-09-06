'use strict';

/**
 * 'Shred' a record into a list of <value, repetition_level, definition_level>
 * tuples per column using the Google Dremel Algorithm.
 */
exports.shredRecords = function(schema, records) {
  let cols = {};

  schema.columns.forEach(function(colDef) {
    cols[colDef.name] = [];
  });

  records.forEach(function(record) {
    shredRecord(schema, record, cols, 0, 0);
  });

  return cols;
}

function shredRecord(schema, record, data, rlvl, dlvl) {
  schema.columns.forEach(function(colDef) {
    let k = colDef.name;

    if (!(k in record)) {
      if (colDef.repetition_type == 'REQUIRED') {
        throw 'missing required column: ' + colDef.name;
      } else {
        data[k].push({
          value: null,
          rlevel: rlvl,
          dlevel: dlvl,
        });
      }

      return;
    }

    if (typeof(record[k]) == 'object') {
      if (colDef.repetition_type != 'REPEATED') {
        throw 'array requires repeated column: ' + colDef.name;
      }

      for (let i = 0; i < record[k].length; ++i) {
        data[k].push({
          value: record[k][i],
          rlevel: i == 0 ? rlvl : colDef.rLevelMax,
          dlevel: colDef.rLevelMax,
        });
      }
    } else {
      data[k].push({
        value: record[k],
        rlevel: rlvl,
        dlevel: colDef.rLevelMax,
      });
    }
  });
}

