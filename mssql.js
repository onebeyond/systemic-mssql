const sql = require('mssql');
const debug = require('debug')('systemic-mssql');
const { sequential } = require('./lib/common');
const fs = require('fs');
const { join, parse } = require('path');

module.exports = config => {
	const start = () => {
		debug('Initializing systemic-mssql');
		return sql.connect({ ...config.db }).then(pool => initApi(pool));
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

	const readFile = (dir, fileName) => fs.readFileSync(join(dir, fileName)).toString();

	const loadSql = location => {
		const fileNames = fs.readdirSync(location);
		return fileNames
			.filter(file => parse(file).ext === '.sql')
			.map(file => ({ name: parse(file).name, query: readFile(location, file) }));
	};

	const initStatements = (query, pool, connectionsPerQuery) => {
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

	const initApi = pool => {
		const queries = loadSql(config.queriesDir);
		const manager = manageStatements(queries, pool);
		const execute = executor(manager);
		return {
			transaction: transaction(pool),
			pool,
			execute,
		};
	};

	return {
		start,
	};
};
