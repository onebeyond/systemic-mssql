const mssql = require('./mssql');

module.exports = () => {
	const start = ({ logger, config }, cb) => {
		mssql(config)
			.start()
			.then(db => cb(null, db))
			.catch(err => {
				logger.error(`${err.message} - ${err.stack}`);
				cb(err);
			});
	};

	return { start };
};
