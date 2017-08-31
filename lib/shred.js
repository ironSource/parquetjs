function shredRecords(schema, records) {
  var cols = {};

  schema.columns.forEach(function(col_def) {
    cols[col_def.name] = [];
  });

  records.forEach(function(record) {
    for (k in cols) {
      cols[k].push({ value: record[k] });
    }
  });

  return cols;
}

module.exports = { shredRecords };

