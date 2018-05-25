const { expect } = require('chai')
const ParquetReadStream = require('../lib/ParquetReadStream')
const parquet = require('../parquet.js')
const path = require('path')
const through2 = require('through2')
const { isBuffer } = require('util')

const TEST_NUM_ROWS = 10000;

describe('ParquetReadStream', () => {
	let reader, stream

	it('reads a parquet file', done => {
		let rows = []
		let processor = through2.obj((chunk, enc, cb) => {
			rows.push(chunk)
			cb()
		})

		stream.pipe(processor)
		stream.on('end', () => {
			expect(rows).to.have.length(TEST_NUM_ROWS)
		})
	})

	beforeEach(async() => {
		reader = await parquet.ParquetReader.openFile(path.resolve(__dirname, '..', 'fruits.parquet'))
		stream = new ParquetReadStream(reader.getCursor())
	})
})