'use strict';

/**
 * 'Shred' a record into a list of <value, repetition_level, definition_level>
 * tuples per column using the Google Dremel Algorithm.
 */
exports.shredRecord = function(schema, record, buffer) {
  /* shred the record, this may raise an exception */
  var recordShredded = {};
  for (let colDef of schema.columns) {
    recordShredded[colDef.name] = [];
  }

  shredRecordInternal(schema, record, recordShredded, 0, 0);

  /* if no error during shredding, add the shredded record to the buffer */
  if (!("columnData" in buffer) || !("rowCount" in buffer)) {
    buffer.rowCount = 0;
    buffer.columnData = {};

    for (let colDef of schema.columns) {
      buffer.columnData[colDef.name] = [];
    }
  }

  buffer.rowCount += 1;
  for (let colDef of schema.columns) {
    Array.prototype.push.apply(
        buffer.columnData[colDef.name],
        recordShredded[colDef.name]);
  }
}

function shredRecordInternal(schema, record, data, rlvl, dlvl) {
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

