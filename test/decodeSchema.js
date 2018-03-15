'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('../parquet.js');

describe('ParquetSchema', function() {
  it('should handle complex nesting', function() {
    var metadata = {
      version: 1,
      schema: [
        { type: null,
          type_length: null,
          repetition_type: null,
          name: 'root',
          num_children: 1,
          converted_type: null,
          scale: null,
          precision: null,
          field_id: null
        }, {
          type: null,
          type_length: null,
          repetition_type: 0,
          name: 'a',
          num_children: 2,
          converted_type: null,
          scale: null,
          precision: null,
          field_id: null
        }, {
          type: null,
          type_length: null,
          repetition_type: 0,
          name: 'b',
          num_children: 2,
          converted_type: null,
          scale: null,
          precision: null,
          field_id: null
        }, {
          type: null,
          type_length: null,
          repetition_type: 0,
          name: 'c',
          num_children: 1,
          converted_type: null,
          scale: null,
          precision: null,
          field_id: null
        }, {
          type: 6,
          type_length: null,
          repetition_type: 0,
          name: 'd',
          num_children: null,
          converted_type: 0,
          scale: null,
          precision: null,
          field_id: null
       }, {
          type: null,
          type_length: null,
          repetition_type: 0,
          name: 'e',
          num_children: 2,
          converted_type: null,
          scale: null,
          precision: null,
          field_id: null
        }, {
          type: 6,
          type_length: null,
          repetition_type: 0,
          name: 'f',
          num_children: null,
          converted_type: 0,
          scale: null,
          precision: null,
          field_id: null
        }, {
          type: 6,
          type_length: null,
          repetition_type: 0,
          name: 'g',
          num_children: null,
          converted_type: 0,
          scale: null,
          precision: null,
          field_id: null
        }, {
          type: 6,
          type_length: null,
          repetition_type: 0,
          name: 'h',
          num_children: null,
          converted_type: 0,
          scale: null,
          precision: null,
          field_id: null
        }
      ]
    };

    const expected = {
      "a": {
        "name": "a",
        "path": [
          "a"
        ],
        "repetitionType": "REQUIRED",
        "statistics": undefined,
        "rLevelMax": 0,
        "dLevelMax": 0,
        "isNested": true,
        "fieldCount": 2,
        "fields": {
          "b": {
            "name": "b",
            "path": [
              "a",
              "b"
            ],
            "repetitionType": "REQUIRED",
            "statistics": undefined,
            "rLevelMax": 0,
            "dLevelMax": 0,
            "isNested": true,
            "fieldCount": 2,
            "fields": {
              "c": {
                "name": "c",
                "path": [
                  "a",
                  "b",
                  "c"
                ],
                "repetitionType": "REQUIRED",
                "statistics": undefined,
                "rLevelMax": 0,
                "dLevelMax": 0,
                "isNested": true,
                "fieldCount": 1,
                "fields": {
                  "d": {
                    "name": "d",
                    "primitiveType": "BYTE_ARRAY",
                    "originalType": "UTF8",
                    "path": [
                      "a",
                      "b",
                      "c",
                      "d"
                    ],
                    "repetitionType": "REQUIRED",
                    "statistics": undefined,
                    "typeLength": undefined,
                    "encoding": "PLAIN",
                    "compression": "UNCOMPRESSED",
                    "rLevelMax": 0,
                    "dLevelMax": 0
                  }
                }
              },
              "e": {
                "name": "e",
                "path": [
                  "a",
                  "b",
                  "e"
                ],
                "repetitionType": "REQUIRED",
                "statistics": undefined,
                "rLevelMax": 0,
                "dLevelMax": 0,
                "isNested": true,
                "fieldCount": 2,
                "fields": {
                  "f": {
                    "name": "f",
                    "primitiveType": "BYTE_ARRAY",
                    "originalType": "UTF8",
                    "path": [
                      "a",
                      "b",
                      "e",
                      "f"
                    ],
                    "repetitionType": "REQUIRED",
                    "statistics": undefined,
                    "typeLength": undefined,
                    "encoding": "PLAIN",
                    "compression": "UNCOMPRESSED",
                    "rLevelMax": 0,
                    "dLevelMax": 0
                  },
                  "g": {
                    "name": "g",
                    "primitiveType": "BYTE_ARRAY",
                    "originalType": "UTF8",
                    "path": [
                      "a",
                      "b",
                      "e",
                      "g"
                    ],
                    "repetitionType": "REQUIRED",
                    "statistics": undefined,
                    "typeLength": undefined,
                    "encoding": "PLAIN",
                    "compression": "UNCOMPRESSED",
                    "rLevelMax": 0,
                    "dLevelMax": 0
                  }
                }
              }
            }
          },
          "h": {
            "name": "h",
            "primitiveType": "BYTE_ARRAY",
            "originalType": "UTF8",
            "path": [
              "a",
              "h"
            ],
            "repetitionType": "REQUIRED",
            "statistics": undefined,
            "typeLength": undefined,
            "encoding": "PLAIN",
            "compression": "UNCOMPRESSED",
            "rLevelMax": 0,
            "dLevelMax": 0
          }
        }
      }
    };

    const reader = new parquet.ParquetReader(metadata,{});
    assert.deepEqual(reader.schema.fields,expected);
  });

});
