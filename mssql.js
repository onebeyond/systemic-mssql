const sql = require('mssql');
const debug = require('debug')('systemic-mssql');
const { sequential } = require('./lib/common');
const fs = require('fs');
const { join, parse } = require('path');

module.exports = ({ logger, config }) => {
	const connectionPool = new sql.ConnectionPool({ ...config.db });

	const timeout = ms => new Promise(resolve => setTimeout(resolve, ms));

	const start = async () => {
		debug('Initializing systemic-mssql');
		let retries = config.retries || 10;
		let connection;
		while ((connection = await startConnection()) === 'nok' && retries-- > 0) await timeout(2500);
		if (connection === 'nok') throw new Error('Unable to connect to DB');
		return connection;
	};

	const startConnection = () =>
		connectionPool
			.connect()
			.then(pool => initApi(pool))
			.catch(err => {
				logger.error(err);
				return 'nok';
			});

	const executeQuery = (transaction, strings, values) => {
		debug('Executing query');
		return transaction.request().query(strings, ...values);
	};

	const transaction = pool => queries => {
		debug('Executing transaction');
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
				throw err;
			});
	};

	const readFile = (dir, fileName) => fs.readFileSync(join(dir, fileName)).toString();

	const loadSql = location => {
		debug(`Loading SQL files from ${location}`);
		const fileNames = fs.readdirSync(location);
		return fileNames
			.filter(file => parse(file).ext === '.sql')
			.map(file => ({ name: parse(file).name, query: readFile(location, file) }));
	};

	const initStatements = (query, pool, connectionsPerQuery) => {
		debug(`Initializing statements for query ${query}`);
		const statements = [];

		for (let i = 0; i < connectionsPerQuery; i++) {
			const getSeriePreparedStatement = new sql.PreparedStatement(pool);
			getSeriePreparedStatement.input('id', sql.Int);
			getSeriePreparedStatement.prepare(query, err => err && console.log(err));
			statements.push(getSeriePreparedStatement);
		}

		return statements;
	};

	const manageStatements = (queries, pool) => {
		const connectionsPerQuery = Math.floor(
			(config.db.pool.max * (config.preparedStatementsSharesPercentage / 100)) / queries.length,
		);
		const statements = queries.reduce(
			(t, c) => ({ ...t, [c.name]: initStatements(c.query, pool, connectionsPerQuery) }),
			{},
		);
		const getStatement = query =>
			new Promise(accept => {
				if (statements[query].length > 0) return accept(statements[query].pop());
				else {
					const t = setInterval(() => {
						if (statements[query].length > 0) {
							clearInterval(t);
							accept(statements[query].pop());
						}
					}, 50);
				}
			});
		const addStatement = (query, statement) => statements[query].push(statement);
		return {
			getStatement,
			addStatement,
		};
	};

	const executor = manager => (query, params) => {
		let statement;
		return manager
			.getStatement(query)
			.then(_statement => {
				statement = _statement;
				return statement.execute(params);
			})
			.then(result => {
				manager.addStatement(query, statement);
				return result;
			});
	};

	const health = pool => () => {
		const start = process.hrtime();
		return pool.request().query('select 1')
			.then(() => {
				debug('Healthcheck ok')
				return {
					status: 'ok',
					response_time_ms: process.hrtime(start)[1] / 1000000,
				};
			})
			.catch(err => {
				debug(`Healthcheck error: ${err.message}`)
				return {
					status: 'error',
					details: err.message,
				}
			})
	};

	const initApi = pool => {
		if (config.queriesDir) {
			const queries = loadSql(config.queriesDir);
			const manager = manageStatements(queries, pool);
			const execute = executor(manager);
			return {
				transaction: transaction(pool),
				pool,
				execute,
			};
		}
		return {
			transaction: transaction(pool),
			health: health(pool),
			pool,
		};
	};

	return {
		start,
	};
};
