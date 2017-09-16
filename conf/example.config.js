module.exports = {
	connectors: {
		'postgres': {
			connection: {
				database: 'test',
				user: 'postgres',
				password: '',
				host: 'localhost',
				port: 5432,
				max: 10
			},

			generateModelsFromSchema: true,
			modelAutogen: true,

			parseInputDatesAsUTC: false,
			schemas: ['public'] // defaults to ['public'] if left omitted
		}
	}
};