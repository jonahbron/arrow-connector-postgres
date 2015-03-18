module.exports = {
	connectors: {
		'postgres': {
			connectionPooling: true,
			connectionLimit: 10,

			database: 'test',
			user: 'root',
			password: '',
			host: 'localhost',
			port: 3306,

			generateModelsFromSchema: true
		}
	}
};