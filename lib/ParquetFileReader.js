const fs = require('fs')
const { promisify } = require('util')
const fopen = promisify(fs.open)
const fstat = promisify(fs.stat)
const fread = promisify(fs.read)

/**
 * @typedef {Object} FileReadResult
 * @property {Number} bytesRead - number of bytes read from the file
 * @property {Buffer} buffer - A buffer containing the data
 */

/**
 * @typedef {Object} Location
 * @property {Number} position - the read start position
 * @property {Buffer} length - number of bytes to read from "position"
 */

/**
 *    A low level implemetation for reading a parquet file.
 *
 *    The ParquetFileReader separates the @ParquetEnvelopeReader from the underlying source.
 *    This should make it easy to implement remote files and other sources of parquet data by either
 *    subclasses this or providing an implementation with a similar interface to the envelope reader
 *    
 */
class ParquetFileReader {

	constructor(uri) {
		this._uri = uri
	}

	/**
	 *    @param  {Location} location 
	 *    @return {FileReadResult} read result
	 */
	async read({ position, length }) {
		if (!this._fd) {
			await this._init()
		}

		let buffer = Buffer.alloc(length)

		return await fread(this._fd, buffer, 0, length, position)
	}

	/**
	 *    @param  {Location[]} locations
	 *    @return {[type]}
	 */
	async readBatch(locations) {

	}

	/**
	 *    @return {[type]}
	 */
	async close() {

	}

	get size() {
		return this._size
	}

	get uri() {
		return this._uri
	}

	async _init() {
		this._size = await fstat(this._uri)
		this._fd = await fopen(this._uri, 'r')
	}
}

module.exports = ParquetFileReader