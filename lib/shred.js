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
    recordShredded[colDef.path] = {
      dlevels: [],
      rlevels: [],
      values: [],
      count: 0
    };
  }

  shredRecordInternal(schema.column_map, record, recordShredded, 0, 0);

  /* if no error during shredding, add the shredded record to the buffer */
  if (!('columnData' in buffer) || !('rowCount' in buffer)) {
    buffer.rowCount = 0;
    buffer.columnData = {};

    for (let colDef of schema.columns) {
      buffer.columnData[colDef.path] = {
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
        buffer.columnData[colDef.path].rlevels,
        recordShredded[colDef.path].rlevels);

    Array.prototype.push.apply(
        buffer.columnData[colDef.path].dlevels,
        recordShredded[colDef.path].dlevels);

    Array.prototype.push.apply(
        buffer.columnData[colDef.path].values,
        recordShredded[colDef.path].values);

    buffer.columnData[colDef.path].count += recordShredded[colDef.path].count;
  }
}

function shredRecordInternal(fields, record, data, rlvl, dlvl) {
  for (let colName in fields) {
    const colDef = fields[colName];
    const colType = colDef.originalType || colDef.primitiveType;

    // fetch values
    let values = [];
    if (record && (colName in record)) {
      if (record[colName].constructor === Array) {
        values = record[colName];
      } else {
        values.push(record[colName]);
      }
    }

    // check values
    if (values.length == 0 && !!record && colDef.repetitionType === 'REQUIRED') {
      console.log(record, values);
      throw 'missing required column: ' + colDef.name;
    }

    if (values.length > 1 && colDef.repetitionType !== 'REPEATED') {
      throw 'too many values for column: ' + colDef.name;
    }

    // push null
    if (values.length == 0) {
      if (colDef.isNested) {
        shredRecordInternal(
            colDef.fields,
            null,
            data,
            rlvl,
            dlvl);
      } else {
        data[colDef.path].rlevels.push(rlvl);
        data[colDef.path].dlevels.push(dlvl);
        data[colDef.path].count += 1;
      }
      continue;
    }

    // push values
    for (let i = 0; i < values.length; ++i) {
      const rlvl_i = i === 0 ? rlvl : colDef.rLevelMax;

      if (colDef.isNested) {
        shredRecordInternal(
            colDef.fields,
            values[i],
            data,
            rlvl_i,
            colDef.dLevelMax);
      } else {
        data[colDef.path].values.push(parquet_types.toPrimitive(colType, values[i]));
        data[colDef.path].rlevels.push(rlvl_i);
        data[colDef.path].dlevels.push(colDef.dLevelMax);
        data[colDef.path].count += 1;
      }
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

