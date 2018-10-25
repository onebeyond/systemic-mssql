const sql = require('mssql');
const debug = require('debug')('systemic-mssql');
const { sequential } = require('./lib/common');

module.exports = config => {
	const start = () => {
		debug('Initializing systemic-mssql');
		return sql.connect({ ...config }).then(pool => initApi(pool));
	};

	const executeQuery = (transaction, strings, values) => transaction.request().query(strings, ...values);

	const transaction = pool => queries => {
		let transaction;
		return pool
			.transaction()
			.begin()
			.then(_transaction => {
				transaction = _transaction;
				return sequential(executeQuery, queries.map(query => [transaction, query.strings, query.values]));
			})
			.then(() => transaction.commit())
			.catch(err => {
				transaction.rollback();
				throw err;
			});
	};

	const initApi = pool => {
		return {
			transaction: transaction(pool),
			pool,
		};
	};

	return {
		start,
	};
};
