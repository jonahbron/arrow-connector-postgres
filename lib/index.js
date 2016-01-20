var _ = require('lodash'),
	async = require('async'),
	pg = require('pg'),
	pkginfo = require('pkginfo')(module) && module.exports,
	defaultConfig = require('fs').readFileSync(__dirname + '/../conf/example.config.js', 'utf8');

// --------- Postgres DB Connector -------

exports.create = function (Arrow, server) {
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
		connect: function (callback) {
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
		disconnect: function (callback) {
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
		fetchSchema: function (callback) {
			// if we already have the schema, just return it
			if (this.schema) { return callback(null, this.schema); }
			this.logger.trace('fetchSchema');
			var query = 'SELECT relname AS table_name, ' +
						'attname::varchar as column_name, ' +
						'CASE WHEN attnotnull THEN \'NO\' ELSE \'YES\' END AS is_nullable, ' +
						'format_type(atttypid, atttypmod) AS data_type, ' +
						'adsrc AS column_default ' +
						'FROM pg_attribute ' +
						'LEFT JOIN pg_class ON pg_class.oid = attrelid ' +
						'LEFT JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace ' +
						'LEFT JOIN pg_attrdef ON pg_attrdef.adrelid = pg_class.oid AND adnum = pg_attribute.attnum ' +
						'WHERE pg_namespace.nspname = \'public\' ' +
						'AND relkind IN (\'r\', \'v\', \'m\') ' +
						'AND pg_attribute.attisdropped = False ' +
						'AND attnum > 0';
			this._query(query, callback, function(results){
				var schema = { objects: {}, database: this.config.database, primary_keys: {} };
				results.rows.forEach(function resultCallback(result) {
					var entry = schema.objects[result.table_name];
					if (!entry) {
						schema.objects[result.table_name] = entry = {};
					}
					entry[result.column_name] = result;
					if (!schema.primary_keys[result.table_name] &&
						(String(result.column_default).match(/^nextval/) ||
						 result.column_name === 'id')) {
						schema.primary_keys[result.table_name] = result.column_name;
					}
					entry[result.column_name].is_auto_increment = String(result.column_default).substr(0, 7) == 'nextval';
				});
				callback(null, schema);
			}.bind(this));
		},
		createModelsFromSchema: function () {
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
						autogen: !!this.config.modelAutogen,
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
		create: function (Model, values, callback) {
			var table = this.getTableName(Model),
				payload = Model.instance(values, false).toPayload(),
				primaryKeyColumn = this.getPrimaryKeyColumn(Model),
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
		findAll: function (Model, callback) {
			var table = this.getTableName(Model),
				primaryKeyColumn = this.getPrimaryKeyColumn(Model),
				query = 'SELECT ' +
					(primaryKeyColumn ? primaryKeyColumn + ', ' : '') + Model.payloadKeys().join(', ') +
					' FROM ' + table + ' ORDER BY ' + primaryKeyColumn + ' LIMIT 1000';
			this._query(query, callback, function (results) {
				var rows = [];
				results.rows.forEach(function rowIterator(row) {
					// var instance = Model.instance(row, true);
					// if (primaryKeyColumn) { instance.setPrimaryKey(row[primaryKeyColumn]); }
					// rows.push(instance);
					rows.push(this.getInstanceFromRow(Model, row));
				}.bind(this));
				callback(null, new Collection(Model, rows));
			}.bind(this));

		},
		findOne: function (Model, id, callback) {
			var table = this.getTableName(Model),
				primaryKeyColumn = this.getPrimaryKeyColumn(Model),
				query = 'SELECT ' +
					(primaryKeyColumn ? primaryKeyColumn + ', ' : '') + Model.payloadKeys().join(', ') +
					' FROM ' + table + ' WHERE ' + primaryKeyColumn + ' = $1 LIMIT 1';
			if (!primaryKeyColumn) {
				return callback(new Arrow.ORMError("can't find primary key column for " + table));
			}
			this._query(query, [id], callback, function(results){
				if (results.rows && results.rows.length) {
					// var row = results.rows[0],
					// 	instance = Model.instance(row, true);
					// instance.setPrimaryKey(id);
					// callback(null, instance);
					callback(null, this.getInstanceFromRow(Model, results.rows[0]));
				}
				else {
					callback();
				}
			}.bind(this));
		},
		query: function (Model, options, callback) {
			// TODO: Parse through this and think about injection attack vectors.
			var key,
				table = this.getTableName(Model),
				primaryKeyColumn = this.getPrimaryKeyColumn(Model),
				keys = {},
				whereQuery = '',
				pagingQuery = '',
				orderQuery = '',
				whereIns = [],
				i,
				values = [];

			if (primaryKeyColumn) {
				keys[primaryKeyColumn] = true;
			}

			var sel = Model.translateKeysForPayload(options.sel),
				unsel = Model.translateKeysForPayload(options.unsel);
			if (sel && Object.keys(sel).length > 0) {
				keys = Object.keys(_.pick(_.merge(keys, sel), Boolean)).join(', ');
			}
			else if (unsel && Object.keys(unsel).length > 0) {
				keys = Object.keys(_.omit(this.getTableSchema(Model), Object.keys(unsel))).join(', ');
			}
			else {
				keys = '*';
			}

			var where = Model.translateKeysForPayload(options.where);
			if (where && Object.keys(where).length > 0) {
				whereQuery = this.whereQuery(where, values);
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
			this._query(query, values, callback, function (results) {
				if (results.rows) {
					var rows = [];
					results.rows.forEach(function rowIterator(row) {
						rows.push(this.getInstanceFromRow(Model, row));
					}.bind(this));
					callback(null, new Collection(Model, rows));
				}
				else {
					callback();
				}
			}.bind(this));
		},
		save: function (Model, instance, callback) {
			var table = this.getTableName(Model),
				payload = instance.toPayload(),
				primaryKeyColumn = this.getPrimaryKeyColumn(Model),
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
				primaryKeyColumn = this.getPrimaryKeyColumn(Model),
				query = 'DELETE FROM ' + table + ' WHERE ' + primaryKeyColumn + ' = $1';
			if (!primaryKeyColumn) {
				return callback(new Arrow.ORMError("can't find primary key column for " + table));
			}
			this._query(query, [instance.getPrimaryKey()], callback, function (result) {
				if (result && result.rowCount) {
					callback(null, instance);
				}
				else {
					callback();
				}
			}.bind(this));
		},
		deleteAll: function (Model, callback) {
			var table = this.getTableName(Model),
				primaryKeyColumn = this.getPrimaryKeyColumn(Model),
				query = 'DELETE FROM ' + table;
			if (!primaryKeyColumn) {
				return callback(new Arrow.ORMError("can't find primary key column for " + table));
			}
			this._query(query, callback, function (result){
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
		whereQuery: function(where, values) {
			var valuePosition = function(value) {
				values.push(value);
				return '$' + String(values.length);
			};

			var whereQuery = '';
			var firstWhere = true;
			for (key in where) {
				if (where.hasOwnProperty(key) && where[key] !== undefined) {
					whereQuery += firstWhere ? ' WHERE' : ' AND';
					whereQuery += ' ' + key;
					firstWhere = false;
					if (where[key] && where[key].$like) {

						whereQuery += ' LIKE ';
						whereQuery += valuePosition(where[key].$like);

					} else if (where[key] && where[key].$in && _.isArray(where[key].$in)) {

						if (where[key].$in.length > 0) {
							whereQuery += ' IN (';
							whereQuery += where[key].$in.map(valuePosition).join(', ');
							whereQuery += ')';
						} else {
							whereQuery += ' = ANY(\'{}\') ';
						}

					} else if (where[key] && where[key].$gt) {

						whereQuery += ' > ';
						whereQuery += valuePosition(where[key].$gt);

					} else if (where[key] && where[key].$gte) {

						whereQuery += ' >= ';
						whereQuery += valuePosition(where[key].$gte);

					} else if (where[key] && where[key].$lt) {

						whereQuery += ' < ';
						whereQuery += valuePosition(where[key].$lt);

					} else if (where[key] && where[key].$lte) {

						whereQuery += ' <= ';
						whereQuery += valuePosition(where[key].$lte);

					} else if (where[key] && where[key].$sql) {

						where[key].$sql = where[key].$sql.replace(/\$([\d]+)/g, function (match, number) {
							return '$' + (Number(number) + values.length);
						});
						values.push.apply(values, where[key].values);
						whereQuery += where[key].$sql;

					} else if (where[key] === null) {

						whereQuery += ' IS NULL';

					} else {

						whereQuery += ' = ';
						whereQuery += valuePosition(where[key]);

					}
				}
			}
			return whereQuery;
		},

		fetchColumns: function fetchColumns(table, payload) {
			if (this.schema.objects[table]) {
				return _.intersection(Object.keys(payload), Object.keys(this.schema.objects[table]));
			}
			return Object.keys(payload);
		},
		getConnection: function getConnection(callback) {
			if (this.connection) {
				callback(null, this.connection, _.noop);
			} else {
				pg.connect(this.connectionConfig, callback);
			}
		},
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
		getPrimaryKeyColumn: function getPrimaryKeyColumn(Model) {
			var pk = Model.getMeta('primarykey');
			if (pk) {
				return pk;
			}
			var name = this.getTableName(Model),
				tableSchema = this.getTableSchema(Model),
				primaryKeyColumn = this.metadata.schema.primary_keys[name],
				column = primaryKeyColumn && tableSchema && tableSchema[primaryKeyColumn];

			return column && column.column_name;
		},
		getTableSchema: function getTableSchema(Model) {
			var name = this.getTableName(Model);
			return this.metadata.schema.objects[name];
		},
		getInstanceFromRow: function (Model, row) {
			var primaryKeyColumn = this.getPrimaryKeyColumn(Model),
				instance = Model.instance(row, true);
			if (primaryKeyColumn) {
				instance.setPrimaryKey(row[primaryKeyColumn]);
			}
			return instance;
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

		_query: function (query, data, callback, executor) {
			if (arguments.length < 4) {
				executor = callback;
				callback = data;
				data = null;
			}
			var logger = this.logger;
			logger.trace('Postgres QUERY=>', query, data);
			this.getConnection(function (error, client, done) {
				if (error) {
					return callback(error);
				}
				client.query(query, data, function (error, results) {
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
