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
  for (let field of schema.fieldList) {
    recordShredded[field.path] = {
      dlevels: [],
      rlevels: [],
      values: [],
      count: 0
    };
  }

  shredRecordInternal(schema.fields, record, recordShredded, 0, 0);

  /* if no error during shredding, add the shredded record to the buffer */
  if (!('columnData' in buffer) || !('rowCount' in buffer)) {
    buffer.rowCount = 0;
    buffer.columnData = {};

    for (let field of schema.fieldList) {
      buffer.columnData[field.path] = {
        dlevels: [],
        rlevels: [],
        values: [],
        count: 0
      };
    }
  }

  buffer.rowCount += 1;
  for (let field of schema.fieldList) {
    Array.prototype.push.apply(
        buffer.columnData[field.path].rlevels,
        recordShredded[field.path].rlevels);

    Array.prototype.push.apply(
        buffer.columnData[field.path].dlevels,
        recordShredded[field.path].dlevels);

    Array.prototype.push.apply(
        buffer.columnData[field.path].values,
        recordShredded[field.path].values);

    buffer.columnData[field.path].count += recordShredded[field.path].count;
  }
}

function shredRecordInternal(fields, record, data, rlvl, dlvl) {
  for (let fieldName in fields) {
    const field = fields[fieldName];
    const fieldType = field.originalType || field.primitiveType;

    // fetch values
    let values = [];
    if (record && (fieldName in record)) {
      if (record[fieldName].constructor === Array) {
        values = record[fieldName];
      } else {
        values.push(record[fieldName]);
      }
    }

    // check values
    if (values.length == 0 && !!record && field.repetitionType === 'REQUIRED') {
      throw 'missing required field: ' + field.name;
    }

    if (values.length > 1 && field.repetitionType !== 'REPEATED') {
      throw 'too many values for field: ' + field.name;
    }

    // push null
    if (values.length == 0) {
      if (field.isNested) {
        shredRecordInternal(
            field.fields,
            null,
            data,
            rlvl,
            dlvl);
      } else {
        data[field.path].rlevels.push(rlvl);
        data[field.path].dlevels.push(dlvl);
        data[field.path].count += 1;
      }
      continue;
    }

    // push values
    for (let i = 0; i < values.length; ++i) {
      const rlvl_i = i === 0 ? rlvl : field.rLevelMax;

      if (field.isNested) {
        shredRecordInternal(
            field.fields,
            values[i],
            data,
            rlvl_i,
            field.dLevelMax);
      } else {
        data[field.path].values.push(parquet_types.toPrimitive(fieldType, values[i]));
        data[field.path].rlevels.push(rlvl_i);
        data[field.path].dlevels.push(field.dLevelMax);
        data[field.path].count += 1;
      }
    }
  }
}

exports.materializeRecords = function(schema, buffer) {
  let records = [];

  let fieldIter = {};
  for (let k in buffer.columnData) {
    fieldIter[k] = {
      rlevels: buffer.columnData[k].rlevels[Symbol.iterator](),
      dlevels: buffer.columnData[k].dlevels[Symbol.iterator](),
      values: buffer.columnData[k].values[Symbol.iterator]()
    }
  }

  for (let i = 0; i < buffer.rowCount; ++i) {
    let record = {};

    for (let field of schema.fieldList) {
      let fieldPath = [field.name];
      if (!(fieldPath in buffer.columnData)) {
        continue;
      }

      let dlevel = fieldIter[fieldPath].dlevels.next().value;
      let rlevel = fieldIter[fieldPath].rlevels.next().value;

      if (dlevel === field.dLevelMax) {
        record[field.name] = parquet_types.fromPrimitive(
            field.originalType,
            fieldIter[fieldPath].values.next().value);

      } else {
        record[field.name] = null;
      }
    }

    records.push(record);
  }

  return records;
}

