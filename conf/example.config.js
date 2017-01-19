module.exports = {
	connectors: {
		'postgres': {
			connectionPooling: true,
			connectionLimit: 10,

			database: 'test',
			user: 'postgres',
			password: '',
			host: 'localhost',
			port: 5432,

			generateModelsFromSchema: true,
			modelAutogen: true,

			parseInputDatesAsUTC: false
		}
	}
};