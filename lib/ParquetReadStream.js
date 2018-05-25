const { Readable } = require('stream')

class ParquetReadStream extends Readable {

	constructor(cursor, opts) {
		super({ objectMode: true })
		this._cursor = cursor
	}

	async _read(size) {
		for (let i = size; i > 0; i--) {
			let row = await this._cursor.next()
			let proceed = this.push(row)
			if (!proceed) return
		}
	}
}

module.exports = ParquetReadStream
