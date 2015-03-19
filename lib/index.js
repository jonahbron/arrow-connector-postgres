var _ = require('lodash'),
	async = require('async'),
	pg = require('pg'),
	pkginfo = require('pkginfo')(module) && module.exports,
	defaultConfig = require('fs').readFileSync(__dirname + '/../conf/example.config.js', 'utf8');

// --------- Postgres DB Connector -------

exports.create = function(Arrow, server) {
	var Connector = Arrow.Connector,
		Collection = Arrow.Collection;

	return Connector.extend({

		/*
		 Configuration.
		 */
		pkginfo: _.pick(pkginfo, 'name', 'version', 'description', 'author', 'license', 'keywords', 'repository'),
		logger: server && server.logger || Arrow.createLogger({}, { name: pkginfo.name }),

		connectionConfig: null,

		/*
		 Lifecycle.
		 */
		connect: function(callback) {
			this.logger.trace('connecting');

			var connected = function connectedCallback(error) {
				if (error) {
					callback(error);
				} else {
					this.logger.trace('connected');
					this.fetchSchema(function fetchedSchema(error, schema) {
						if (error) {
							callback(error);
						} else {
							this.schema = schema;
							if (this.config.generateModelsFromSchema === undefined || this.config.generateModelsFromSchema) {
								this.createModelsFromSchema();
							}
							callback();
						}
					}.bind(this));
				}
			}.bind(this);

			this.connectionConfig = this.config.connection || this.config;

			if (this.config.connectionPooling) {
				pg.defaults.poolSize = this.config.connectionLimit;
				this.logger.trace('created Postgres connection pool');
				pg.connect(this.connectionConfig, function connectPool(error, client, done) {
					if (error) {
						callback(error);
					} else {
						done();
						connected(error);
					}
				}.bind(this));
			}
			else {
				this.logger.trace('not using a Postgres connection pool');
				this.connection = new pg.Client(this.connectionConfig);
				this.connection.connect(connected);
			}
		},
		disconnect: function(callback) {
			this.logger.trace('disconnecting');
			(this.connection || pg).end(function() {
				this.logger.trace('disconnected');
				callback();
			}.bind(this));
		},

		/*
		 Metadata.
		 */
		defaultConfig: defaultConfig,
		fetchSchema: function(callback) {
			// if we already have the schema, just return it
			if (this.schema) { return callback(null, this.schema); }
			this.logger.trace('fetchSchema');
			var query = 'SELECT COLUMNS.table_name, COLUMNS.column_name, COLUMNS.is_nullable, COLUMNS.data_type, COLUMNS.column_default, TABLE_CONSTRAINTS.constraint_type ' +
						'FROM INFORMATION_SCHEMA.COLUMNS ' +
						'LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ' +
						'ON KEY_COLUMN_USAGE.table_schema = COLUMNS.table_schema ' +
						'AND KEY_COLUMN_USAGE.table_name = COLUMNS.table_name ' +
						'AND KEY_COLUMN_USAGE.ordinal_position = COLUMNS.ordinal_position ' +
						'LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS ' +
						'ON TABLE_CONSTRAINTS.constraint_name = KEY_COLUMN_USAGE.constraint_name ' +
						'WHERE COLUMNS.table_schema = \'public\'';
			this._query(query, callback, function(results){
				var schema = { objects: {}, database: this.config.database, primary_keys: {} };
				results.rows.forEach(function resultCallback(result) {
					var entry = schema.objects[result.table_name];
					if (!entry) {
						schema.objects[result.table_name] = entry = {};
					}
					entry[result.column_name] = result;
					if (result.constraint_type === 'PRIMARY KEY') {
						schema.primary_keys[result.table_name] = result.column_name;
					}
					entry[result.column_name].is_auto_increment = String(result.column_default).substr(0, 7) == 'nextval';
				});
				callback(null, schema);
			}.bind(this));
		},
		createModelsFromSchema: function() {
			var models = {};
			for (var modelName in this.schema.objects) {
				if (this.schema.objects.hasOwnProperty(modelName)) {
					var object = this.schema.objects[modelName],
						fields = {};
					for (var fieldName in object) {
						if (object.hasOwnProperty(fieldName)) {
							if (fieldName === 'id') {
								continue;
							}
							fields[fieldName] = {
								type: this.convertDataTypeToJSType(object[fieldName].data_type),
								required: object.is_nullable === 'NO'
							};
						}
					}

					var Model = Arrow.Model.extend(pkginfo.name + '/' + modelName, {
						name: pkginfo.name + '/' + modelName,
						autogen: this.config.modelAutogen === undefined ? true : this.config.modelAutogen,
						fields: fields,
						connector: this,
						generated: true
					});
					models[pkginfo.name + '/' + modelName] = Model;
					if (server) {
						server.addModel(Model);
					}
				}
			}
			this.models = _.defaults(this.models || {}, models);
			if (server) {
				server.registerModelsForConnector(this, this.models);
			}
		},

		/*
		 CRUD.
		 */
		create: function(Model, values, callback) {
			var table = this.getTableName(Model),
				payload = Model.instance(values, false).toPayload(),
				primaryKeyColumn = this.getPrimaryKeyColumn(this, Model),
				columns = this.fetchColumns(table, payload),
				placeholders = columns.map(function(value, index) { return '$' + (index + 1); }),
				query;

			query = 'INSERT INTO ' + table + ' (' + columns.join(',') + ') VALUES (' + placeholders.join(',') + ') RETURNING ' + primaryKeyColumn;
			var data = _.values(_.pick(payload, columns));
			this._query(query, data, callback, function(result){
				if (result && result.rowCount) {
					var instance = Model.instance(values),
						primaryKey = primaryKeyColumn && this.metadata.schema.objects[table][primaryKeyColumn];
					// if this is an auto_increment int primary key, we can just set it from result,
					// otherwise we need to fetch it
					if (primaryKey) {
						if (primaryKey.is_auto_increment) {
							instance.setPrimaryKey(result.rows[0][primaryKeyColumn]);
							callback(null, instance);
						}
						else {
							//TODO: not sure what to do for this...
							this.logger.warn("Not sure how to handle result with non auto_increment primary key type", query);
							callback(new Arrow.ORMError("Not sure how to handle result with non auto_increment primary key type"));
						}
					}
					else {
						callback(null, instance);
					}
				}
				else {
					callback();
				}
			}.bind(this));
		},
		findAll: function(Model, callback) {
			var table = this.getTableName(Model),
				primaryKeyColumn = this.getPrimaryKeyColumn(this, Model),
				query = 'SELECT ' +
					(primaryKeyColumn ? primaryKeyColumn + ', ' + Model.payloadKeys().join(', ') : Model.payloadKeys().join(', ')) +
					' FROM ' + table + ' ORDER BY ' + primaryKeyColumn + ' LIMIT 1000';
			this._query(query, callback, function(results){
				var rows = [];
				results.rows.forEach(function rowIterator(row) {
					var instance = Model.instance(row, true);
					if (primaryKeyColumn) { instance.setPrimaryKey(row[primaryKeyColumn]); }
					rows.push(instance);
				});
				callback(null, new Collection(Model, rows));
			}.bind(this));

		},
		findOne: function(Model, id, callback) {
			var table = this.getTableName(Model),
				primaryKeyColumn = this.getPrimaryKeyColumn(this, Model),
				query = 'SELECT ' + Model.payloadKeys().join(', ') + ' FROM ' + table + ' WHERE ' + primaryKeyColumn + ' = $1 LIMIT 1';
			if (!primaryKeyColumn) {
				return callback(new Arrow.ORMError("can't find primary key column for " + table));
			}
			this._query(query, [id], callback, function(results){
				if (results.rows && results.rows.length) {
					var row = results.rows[0],
						instance = Model.instance(row, true);
					instance.setPrimaryKey(id);
					callback(null, instance);
				}
				else {
					callback();
				}
			}.bind(this));
		},
		query: function(Model, options, callback) {
			// TODO: Parse through this and think about injection attack vectors.
			var key,
				table = this.getTableName(Model),
				primaryKeyColumn = this.getPrimaryKeyColumn(this, Model),
				keys = primaryKeyColumn,
				whereQuery = '',
				pagingQuery = '',
				orderQuery = '',
				values = [];

			var sel = Model.translateKeysForPayload(options.sel),
				unsel = Model.translateKeysForPayload(options.unsel);
			if (sel && Object.keys(sel).length > 0) {
				keys += ', ' + _.keys(_.omit(sel, primaryKeyColumn)).join(', ');
			}
			else if (unsel && Object.keys(unsel).length > 0) {
				keys += ', ' + _.keys(_.omit(_.omit(this.getTableSchema(this, Model), primaryKeyColumn), _.keys(unsel))).join(', ');
			}
			else {
				keys = '*';
			}

			var where = Model.translateKeysForPayload(options.where);
			if (where && Object.keys(where).length > 0) {
				var firstWhere = true;
				var parameterPosition = 1;
				for (key in where) {
					if (where.hasOwnProperty(key) && where[key] !== undefined) {
						if (firstWhere) {
							whereQuery = ' WHERE';
						}
						if (!firstWhere) {
							whereQuery += ' AND';
						}
						whereQuery += ' ' + key;
						firstWhere = false;
						if (where[key] && where[key].$like) {
							whereQuery += ' LIKE';
							values.push(where[key].$like);
						}
						else {
							whereQuery += ' =';
							values.push(where[key]);
						}
						whereQuery += ' $' + (parameterPosition++);
					}
				}
			}

			var order = Model.translateKeysForPayload(options.order);
			if (order && Object.keys(order).length > 0) {
				orderQuery = ' ORDER BY';
				for (key in order) {
					if (order.hasOwnProperty(key)) {
						orderQuery += ' ' + key + ' ';
						if (order[key] == 1) {
							orderQuery += 'ASC';
						}
						else {
							orderQuery += 'DESC';
						}
						orderQuery += ',';
					}
				}
				if (orderQuery[orderQuery.length - 1] === ',') {
					orderQuery = orderQuery.slice(0, -1);
				}
			}

			pagingQuery += ' LIMIT ' + (+options.limit);
			if (options.skip) {
				pagingQuery += ' OFFSET ' + (+options.skip);
			}

			var query = 'SELECT ' + keys + ' FROM ' + table + whereQuery + orderQuery + pagingQuery;
			this._query(query, values, callback, function(results){
				if (results.rows) {
					var rows = [];
					results.rows.forEach(function rowIterator(row) {
						var instance = Model.instance(row, true);
						if (primaryKeyColumn) {
							instance.setPrimaryKey(row[primaryKeyColumn]);
						}
						rows.push(instance);
					});
					callback(null, new Collection(Model, rows));
				}
				else {
					callback();
				}
			}.bind(this));
		},
		save: function(Model, instance, callback) {
			var table = this.getTableName(Model),
				payload = instance.toPayload(),
				primaryKeyColumn = this.getPrimaryKeyColumn(this, Model),
				columns = this.fetchColumns(table, payload),
				placeholders = columns.map(function(name, index) { return name + ' = $' + (index + 1); }),
				query = 'UPDATE ' + table + ' SET ' + placeholders.join(',') + ' WHERE ' + primaryKeyColumn + ' = $' + (placeholders.length + 1);
			if (!primaryKeyColumn) {
				return callback(new Arrow.ORMError("can't find primary key column for " + table));
			}
			var values = _.values(payload).concat([instance.getPrimaryKey()]);
			this._query(query, values, callback, function(result){
				if (result.rows && result.rowCount) {
					callback(null, instance);
				}
				else {
					callback();
				}
			}.bind(this));
		},
		delete: function(Model, instance, callback) {
			var table = this.getTableName(Model),
				primaryKeyColumn = this.getPrimaryKeyColumn(this, Model),
				query = 'DELETE FROM ' + table + ' WHERE ' + primaryKeyColumn + ' = $1';
			if (!primaryKeyColumn) {
				return callback(new Arrow.ORMError("can't find primary key column for " + table));
			}
			this._query(query, [instance.getPrimaryKey()], callback, function(result){
				if (result && result.rowCount) {
					callback(null, instance);
				}
				else {
					callback();
				}
			}.bind(this));
		},
		deleteAll: function(Model, callback) {
			var table = this.getTableName(Model),
				primaryKeyColumn = this.getPrimaryKeyColumn(this, Model),
				query = 'DELETE FROM ' + table;
			if (!primaryKeyColumn) {
				return callback(new Arrow.ORMError("can't find primary key column for " + table));
			}
			this._query(query, callback, function(result){
				if (result && result.rowCount) {
					callback(null, result.rowCount);
				}
				else {
					callback();
				}
			}.bind(this));
		},

		/*
		 Utilities only used for this connector.
		 */
		getTableName: function getTableName(Model) {
			var parent = Model;
			while (parent._parent && parent._parent.name) {
				parent = parent._parent;
			}
			var table = Model.getMeta('table') || parent.name || Model._supermodel || Model.name;
			if (table.indexOf(pkginfo.name + '/') >= 0) {
				table = table.replace(pkginfo.name + '/', '');
			}
			return table;
		},
		fetchColumns: function fetchColumns(table, payload) {
			return _.intersection(Object.keys(payload), Object.keys(this.schema.objects[table]));
		},
		getConnection: function getConnection(callback) {
			if (this.connection) {
				callback(null, this.connection, _.noop);
			} else {
				pg.connect(this.connectionConfig, callback);
			}
		},
		getPrimaryKeyColumn: function getPrimaryKeyColumn(connector, Model) {
			var pk = Model.getMeta('primarykey');
			if (pk) {
				return pk;
			}
			var name = this.getTableName(Model),
				tableSchema = this.getTableSchema(connector, Model),
				primaryKeyColumn = connector.metadata.schema.primary_keys[name],
				column = primaryKeyColumn && tableSchema && tableSchema[primaryKeyColumn];

			return column && column.column_name;
		},
		getTableSchema: function getTableSchema(connector, Model) {
			var name = this.getTableName(Model);
			return connector.metadata.schema.objects[name];
		},
		convertDataTypeToJSType: function convertDataTypeToJSType(dataType) {
			switch (dataType) {
				case 'tinyint':
				case 'smallint':
				case 'mediumint':
				case 'bigint':
				case 'int':
				case 'integer':
				case 'float':
				case 'bit':
				case 'double':
				case 'binary':
					return Number;
				case 'date':
				case 'datetime':
				case 'time':
				case 'year':
					return Date;
				default:
					return String;
			}
		},

		_query: function(query, data, callback, executor) {
			if (arguments.length < 4) {
				executor = callback;
				callback = data;
				data = null;
			}
			var logger = this.logger;
			logger.trace('Postgres QUERY=>', query, data);
			this.getConnection(function(error, client, done) {
				if (error) {
					return callback(error);
				}
				client.query(query, data, function(error, results) {
					try {
						logger.trace('connection released back to the pool');
						done();
					} catch (E) { }

					if (error) {
						callback(error);
					} else {
						logger.trace(results);
						executor(results);
					}
				});
			});
		}

	});

};
