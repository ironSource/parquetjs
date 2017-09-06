'use strict';

/**
 * 'Shred' a record into a list of <value, repetition_level, definition_level>
 * tuples per column using the Google Dremel Algorithm..
 *
 * The buffer argument must point to an object into which the shredded record
 * will be returned. You may re-use the buffer for repeated calls to this function
 * to append to an existing buffer, as long as the schema is unchanged.
 *
 * The format in which the shredded records will be stored in the buffer is as
 * follows:
 *
 *   buffer = {
 *     columnData: [
 *       "my_col": [
 *          { value: v1, dlevel: d1, rlevel: r1 },
 *          { value: v2, dlevel: d2, rlevel: r2 },
 *          ...
 *          { value: vN, dlevel: dN, rlevel: rN },
 *        ],
 *        ...
 *      ],
 *      rowCount: X,
 *   }
 *
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

