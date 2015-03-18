module.exports = {
	connectors: {
		'postgres': {
			connectionPooling: true,
			connectionLimit: 10,

			database: 'test',
			user: 'root',
			password: '',
			host: 'localhost',
			port: 5432,

			generateModelsFromSchema: true
		}
	}
};