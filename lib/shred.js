'use strict';
const parquet_types = require('./types');
const parquet_schema = require('./schema');

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
 *       'my_col': {
 *          dlevels: [d1, d2, .. dN],
 *          rlevels: [r1, r2, .. rN],
 *          values: [v1, v2, .. vN],
 *        }, ...
 *      ],
 *      rowCount: X,
 *   }
 *
 */
exports.shredRecord = function(schema, record, buffer) {
  /* shred the record, this may raise an exception */
  var recordShredded = {};
  for (let colDef of schema.columns) {
    recordShredded[colDef.name] = {
      dlevels: [],
      rlevels: [],
      values: [],
      count: 0
    };
  }

  shredRecordInternal(schema, record, recordShredded, 0, 0);

  /* if no error during shredding, add the shredded record to the buffer */
  if (!('columnData' in buffer) || !('rowCount' in buffer)) {
    buffer.rowCount = 0;
    buffer.columnData = {};

    for (let colDef of schema.columns) {
      buffer.columnData[colDef.name] = {
        dlevels: [],
        rlevels: [],
        values: [],
        count: 0
      };
    }
  }

  buffer.rowCount += 1;
  for (let colDef of schema.columns) {
    Array.prototype.push.apply(
        buffer.columnData[colDef.name].rlevels,
        recordShredded[colDef.name].rlevels);

    Array.prototype.push.apply(
        buffer.columnData[colDef.name].dlevels,
        recordShredded[colDef.name].dlevels);

    Array.prototype.push.apply(
        buffer.columnData[colDef.name].values,
        recordShredded[colDef.name].values);

    buffer.columnData[colDef.name].count += recordShredded[colDef.name].count;
  }
}

function shredRecordInternal(schema, record, data, rlvl, dlvl) {
  for (let colDef of schema.columns) {
    var colType = colDef.originalType || colDef.primitiveType;
    let k = colDef.name;

    if (!(k in record)) {
      if (colDef.repetitionType === 'REQUIRED') {
        throw 'missing required column: ' + colDef.name;
      } else {
        data[k].rlevels.push(rlvl);
        data[k].dlevels.push(dlvl);
        data[k].count += 1;
      }

      continue;
    }

    if (record[k].constructor === Array && colDef.repetitionType === 'REPEATED') {
      for (let i = 0; i < record[k].length; ++i) {
        data[k].values.push(parquet_types.toPrimitive(colType, record[k][i]));
        data[k].rlevels.push(i === 0 ? rlvl : colDef.rLevelMax);
        data[k].dlevels.push(colDef.dLevelMax);
        data[k].count += 1;
      }
    } else {
      data[k].values.push(parquet_types.toPrimitive(colType, record[k]));
      data[k].rlevels.push(rlvl);
      data[k].dlevels.push(colDef.dLevelMax);
      data[k].count += 1;
    }
  }
}

exports.materializeRecords = function(schema, buffer) {
  let records = [];

  let columnIter = {};
  for (let k in buffer.columnData) {
    columnIter[k] = {
      rlevels: buffer.columnData[k].rlevels[Symbol.iterator](),
      dlevels: buffer.columnData[k].dlevels[Symbol.iterator](),
      values: buffer.columnData[k].values[Symbol.iterator]()
    }
  }

  for (let i = 0; i < buffer.rowCount; ++i) {
    let record = {};

    for (let colDef of schema.columns) {
      let colPath = parquet_schema.getColumnKey([colDef.name]);
      if (!(colPath in buffer.columnData)) {
        continue;
      }

      let dlevel = columnIter[colPath].dlevels.next().value;
      let rlevel = columnIter[colPath].rlevels.next().value;

      if (dlevel === colDef.dLevelMax) {
        record[colDef.name] = parquet_types.fromPrimitive(
            colDef.originalType,
            columnIter[colPath].values.next().value);

      } else {
        record[colDef.name] = null;
      }
    }

    records.push(record);
  }

  return records;
}

