'use strict';

/**
 * 'Shred' a record into a list of <value, repetition_level, definition_level>
 * tuples per column using the Google Dremel Algorithm.
 */
exports.shredRecords = function(schema, records) {
  let cols = {};

  for (let colDef of schema.columns) {
    cols[colDef.name] = [];
  }

  for (let record of records) {
    shredRecord(schema, record, cols, 0, 0);
  }

  return cols;
}

function shredRecord(schema, record, data, rlvl, dlvl) {
  for (let colDef of schema.columns) {
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

      continue;
    }

    if (typeof(record[k]) == 'object') {
      if (colDef.repetition_type != 'REPEATED') {
        throw 'array requires repeated column: ' + colDef.name;
      }

      for (let i = 0; i < record[k].length; ++i) {
        data[k].push({
          value: record[k][i],
          rlevel: i == 0 ? rlvl : colDef.rLevelMax,
          dlevel: colDef.dLevelMax,
        });
      }
    } else {
      data[k].push({
        value: record[k],
        rlevel: rlvl,
        dlevel: colDef.dLevelMax,
      });
    }
  }
}

