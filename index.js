const mssql = require('./mssql');

module.exports = () => {
	let db;
	const start = ({ logger, config }, cb) => {
		mssql({ logger, config })
			.start()
			.then(_db => {
				db = _db;
				cb(null, db);
			})
			.catch(err => {
				logger.error(`${err.message} - ${err.stack}`);
				cb(err);
			});
	};

	const stop = cb => {
		db.pool && db.pool.close();
		cb(null);
	};

	return { start, stop };
};
