
var _ = require('lodash');
var Pool = require('pg').Pool;
var pkginfo = require('pkginfo')(module) && module.exports;
var defaultConfig = require('fs').readFileSync(__dirname + '/../conf/example.config.js', 'utf8');
var pgDefaults = require('pg/lib/defaults');
var ConnectionParameters = require('pg/lib/connection-parameters');
var ObjectID = require('bson-objectid');

// --------- Postgres DB Connector -------

exports.create = function (Arrow, server) {
	var Connector = Arrow.Connector;
	var Collection = Arrow.Collection;

	return Connector.extend({

		/*
		 Configuration.
		 */
		pkginfo: _.pick(pkginfo, 'name', 'version', 'description', 'author', 'license', 'keywords', 'repository'),
		logger: server && server.logger || Arrow.createLogger({}, { name: pkginfo.name }),

		connectionPool: null,
		readReplicaConnectionPools: [],

		/*
		 Lifecycle.
		 */

		connect: function (callback) {
			this.logger.trace('connecting');

			var connectedCallback = function connectedCallback(error) {
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

			this.connectionParameters = new ConnectionParameters(this.config.connection);

			if (this.config.parseInputDatesAsUTC) {
				pgDefaults.parseInputDatesAsUTC = true;
			}

			if (!this.config.schemas) {
				this.config.schemas = ['public'];
			} else if (this.config.schemas.length === 0) {
				this.logger.trace('No schemas specified');
			}

			this.connectionPool = new Pool(this.config.connection);
			if (this.config.readReplicaHosts) {
				this.readReplicaConnectionPools = this.config.readReplicaHosts.map(function (host) {
					return new Pool(_.merge({host: host}, this.config.connection));
				}.bind(this));
			}

			this.logger.trace('created Postgres connection pool');

			// Attempt connection to ensure settings are correct, then load schema
			this.connectionPool.connect(function connectPool(error, client, release) {
				if (error) {
					callback(error);
				} else {
					release();
					connectedCallback(error);
				}
			}.bind(this));
		},
		disconnect: function (callback) {
			this.logger.trace('disconnecting');

			this.readReplicaConnectionPools.forEach(function (pool) {
				pool.end();
			});

			this.connectionPool.end(function () {
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
			if (this.schema) {
				return callback(null, this.schema);
			}
			this.logger.trace('fetchSchema');
			var query = 'SHOW search_path';

			// Need search_path first to know which tables need explicit namespace
			this._query(query, null, null, true, callback, function (results) {
				var search_path = results
					.rows
					.shift()
					.search_path
					.replace('"$user"', this.connectionParameters.user)
					.split(',')
					.reduce(function (schemas, schema) {
						schemas[schema] = true;
						return schemas;
					}, {});
				var query = 'SELECT relname AS table_name, ' +
							'attname::varchar as column_name, ' +
							'CASE WHEN attnotnull THEN \'NO\' ELSE \'YES\' END AS is_nullable, ' +
							'format_type(atttypid, atttypmod) AS data_type, ' +
							'adsrc AS column_default, ' +
							'pg_namespace.nspname AS schema_name ' +
							'FROM pg_attribute ' +
							'LEFT JOIN pg_class ON pg_class.oid = attrelid ' +
							'LEFT JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace ' +
							'LEFT JOIN pg_attrdef ON '+
							'pg_attrdef.adrelid = pg_class.oid AND adnum = pg_attribute.attnum ' +
							'WHERE pg_namespace.nspname = ANY(\'{' + this.config.schemas.join(', ') + '}\') ' +
							'AND relkind IN (\'r\', \'v\', \'m\') ' +
							'AND pg_attribute.attisdropped = False ' +
							'AND attnum > 0';
				this._query(query, null, null, true, callback, function(results) {
					var schema = { objects: {}, database: this.config.database, primary_keys: {} };
					results.rows.forEach(function resultCallback(result) {
						var table_name = namespaceTable(result.table_name, result.schema_name, search_path);
						var entry = schema.objects[table_name];
						if (!entry) {
							schema.objects[table_name] = entry = {};
						}
						entry[result.column_name] = result;
						if (!schema.primary_keys[table_name] &&
							(String(result.column_default).match(/^nextval/) ||
							 result.column_name === 'id')) {
							schema.primary_keys[table_name] = result.column_name;
						}
						entry[result.column_name].is_auto_increment = String(result.column_default).substr(0, 7) ==
						                                              'nextval';
					});
					callback(null, schema);
				}.bind(this));
			}.bind(this));
		},
		createModelsFromSchema: function () {
			var models = {};
			for (var modelName in this.schema.objects) {
				if (this.schema.objects.hasOwnProperty(modelName)) {
				    var connectorName = this.config.alias || pkginfo.name;
					var fullModelName = connectorName + '/' + modelName;
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

					// Bypass Arrow's extend functionality to enable validation skipping
					var model = new Arrow.Model(
						fullModelName,
						{
							name: fullModelName,
							autogen: Boolean(this.config.modelAutogen),
							fields: fields,
							connector: this,
							generated: true
						},
						true
					);

					// Normally handled by Arrow's extend function, must be done explicitly
					this.emit('init-model', model);

					models[fullModelName] = model;
					if (server) {
						server.addModel(model);
					}
				}
			}
			this.models = _.defaults(this.models || {}, models);
			if (server) {
				server.registerModelsForConnector(this, this.models);
			}
		},

        _groomPayload: function(Model, payload) {
            // support json arrays stored as jsonb fields in postgres
            if (Model !== undefined && Model !== null) {
                _.forEach(payload, function (value, key) {
                    if (Model.fields[key] && Model.fields[key].toPostgresWrapper) {
                        var jsonWrapper = {
                            data: value,
                            toPostgres: function () {
                                return JSON.stringify(this.data);
                            }
                        };
                        payload[key] = jsonWrapper;
                    }
                });
            }
            return payload;
        },

		/*
		 CRUD.
		 */
		create: function (Model, values, callback) {
            var table = this.getTableName(Model);
			var payload = Model.instance(values, false).toPayload();
			var primaryKeyColumn = this.getPrimaryKeyColumn(Model);

			// generate a new object id
            payload[primaryKeyColumn] = ObjectID.generate();
            values[primaryKeyColumn] = payload[primaryKeyColumn];

            var	columns = this.fetchColumns(table, payload);
            columns.unshift(primaryKeyColumn);
            columns.pop();

            var placeholders = columns.map(function(value, index) { return '$' + (index + 1); });

            payload = this._groomPayload(Model, payload);

			var query = 'INSERT INTO ' + table + ' (' + columns.join(',') + ') VALUES (' + placeholders.join(',') + ') RETURNING ' + primaryKeyColumn;
			var data = _.values(_.pick(payload, columns));

			var request = this._getRequest(Model);
			var response = this._getResponse(Model);

			this._query(query, data, request, response, callback, function(result) {
				if (result && result.rowCount) {
					var instance = Model.instance(values);
                    instance._values.id = values.id;
                    instance.setPrimaryKey(result.rows[0][primaryKeyColumn]);
                    callback(null, instance);
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

			var request = this._getRequest(Model);
			var response = this._getResponse(Model);

			this._query(query, request, response, true, callback, function (results) {
				var rows = [];
				results.rows.forEach(function rowIterator(row) {
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

			var request = this._getRequest(Model);
			var response = this._getResponse(Model);

			this._query(query, [id], request, response, true, callback, function(results) {
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
				whereQuery = this.whereQuery(where, values, Model);
			}

			var order = Model.translateKeysForPayload(options.order);
            if (order && (typeof order !== 'object')) {

                var orderKeys = order.split(",");
                orderQuery = ' ORDER BY ';

                _.forEach(orderKeys, function(order_key) {
                    if (order_key.startsWith('-')) {
                        order_key = order_key.substring(1);
                        orderQuery += order_key + ' DESC';
                    } else {
                        orderQuery += order_key + ' ASC';
                    }
                    orderQuery += ',';
                });
            } else if (order && Object.keys(order).length > 0) {
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
			}
            if (orderQuery[orderQuery.length - 1] === ',') {
                orderQuery = orderQuery.slice(0, -1);
            }

			var query = 'SELECT ' + keys + ' FROM ' + table + whereQuery + orderQuery + pagingQuery;

			if (options.limit && options.limit > 0) {
				values.push(options.limit);
				query += ' LIMIT $' + String(values.length);
			}

			var request = this._getRequest(Model);
			var response = this._getResponse(Model);

			this._query(query, values, request, response, true, callback, function (results) {
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
            var primaryKeyColumn = this.getPrimaryKeyColumn(Model);
            var payload, primaryKey;

            if (typeof instance.toPayload === "function") {
                payload = instance.toPayload();
                primaryKey = instance.getPrimaryKey();
            } else {
                payload = instance;
                primaryKey = instance[primaryKeyColumn];
                delete payload[primaryKeyColumn];
            }
			var table = this.getTableName(Model),
				columns = this.fetchColumns(table, payload),
				placeholders = columns.map(function(name, index) { return name + ' = $' + (index + 1); }),
				query = 'UPDATE ' + table + ' SET ' + placeholders.join(',') + ' WHERE ' + primaryKeyColumn + ' = $' + (placeholders.length + 1);
			if (!primaryKeyColumn) {
				return callback(new Arrow.ORMError("can't find primary key column for " + table));
			}

			payload = this._groomPayload(Model, payload);

			var values = _.values(payload).concat([primaryKey]);

			var request = this._getRequest(Model);
			var response = this._getResponse(Model);

			this._query(query, values, request, response, callback, function(result) {
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

			var request = this._getRequest(Model);
			var response = this._getResponse(Model);

			this._query(query, [instance.getPrimaryKey()], request, response, callback, function (result) {
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

			var request = this._getRequest(Model);
			var response = this._getResponse(Model);

			this._query(query, request, response, callback, function (result) {
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
		whereQuery: function(where, values, model) {
			var valuePosition = function(value) {
				values.push(value);
				return '$' + String(values.length);
			};

			var whereQuery = '';
			var firstWhere = true;
			for (key in where) {
				if (where.hasOwnProperty(key) && where[key] !== undefined) {
				    if (key !== '$or' && key !== '$nor') {
                        whereQuery += firstWhere ? ' WHERE' : ' AND';
                        whereQuery += ' ' + key;
                        firstWhere = false;
                    }
					if (where[key] && where[key].$like) {

						whereQuery += ' LIKE ';
						whereQuery += valuePosition(where[key].$like);

					} else if (where[key] && where[key].$in && _.isArray(where[key].$in)) {

                        // support array query syntax for in clauses
                        var tableSchema = this.getTableSchema(model);
                        if (tableSchema !== undefined && tableSchema[key] && tableSchema[key].data_type === 'text[]') {
                            whereQuery += ' && (ARRAY[';
                            if (where[key].$in.length > 0) {
                                whereQuery += where[key].$in.map(valuePosition).join(', ');
                                whereQuery += '])';
                            } else {
                                whereQuery += ']::text[])';
                            }
                        } else if (where[key].$in.length > 0) {
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

                    } else if (key ==='$or' || key ==='$nor') {
                        if (where[key].length > 0) {

                            whereQuery += firstWhere ? ' WHERE ' : ' AND ';
                            firstWhere = false;

                            whereQuery += where[key].map(function (entry) {
                                return _.map(entry, function (entryValue, entryKey) {
                                    return entryKey + ((key ==='$or') ? ' = ' : ' <> ') + valuePosition(entryValue);
                                }).join(' AND ');
                            }).join(' OR ');
                        }

					} else if (where[key] && where[key].$sql) {

                        where[key].$sql = where[key].$sql.replace(/\$([\d]+)/g, function (match, number) {
                            return '$' + (Number(number) + values.length);
                        });
                        values.push.apply(values, where[key].values);
                        whereQuery += where[key].$sql;

                    } else if (where[key] && where[key].$exists !== undefined) {

				        if (where[key].$exists) {
                            whereQuery += ' IS NOT NULL';
                        } else {
                            whereQuery += ' IS NULL';
                        }

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
			// this is disabled intentionally until we can get the ordering fixed
			// if (false && this.schema.objects[table]) {
			// 	return _.intersection(Object.keys(payload), Object.keys(this.schema.objects[table]));
			// }
			return Object.keys(payload);
		},

		/**
		 * Get the connection, or retrieve a connection from the pool.  If a onAllocateConnection callback is
		 * configured, it will be called before the connection is handed back.
		 */
		getConnection: function getConnection(callback, request, response, readOnly) {
			var pool;
			if (readOnly && this.readReplicaConnectionPools.length > 0) {
				pool = this.getReadReplicaPool();
			} else {
				pool = this.connectionPool;
			}
			pool.connect(
				allocateConnection.bind(null, this.config, request, response, this.logger, callback)
			);
		},

		/**
		 * Get least loaded pool, determined by available connections and remaining space in the pool
		 *
		 * @return {Object} pool
		 */
		getReadReplicaPool: function getReadReplicaPool() {
			var sortedPools = _.sortBy(this.readReplicaConnectionPools, [poolAvailableConnections, poolAvailableSpace]);

			var bestPool = _.last(sortedPools);

			return bestPool;
		},
		getTableName: function getTableName(Model) {
			var parent = Model;
			while (parent._parent && parent._parent.name) {
				parent = parent._parent;
			}
			var table = Model.getMeta('table') || parent.name || Model._supermodel || Model.name;

			var connectorName = this.config.alias || pkginfo.name;

			if (table.indexOf(connectorName + '/') >= 0) {
				table = table.replace(connectorName + '/', '');
			}
			return table;
		},
		getPrimaryKeyColumn: function getPrimaryKeyColumn(Model) {
			var pk = Model.getMeta('primarykey', 'id');
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

			instance._values.id = row.id;

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

		_query: function (query, data, request, response, readOnly, callback, executor) {

			// data argument is optional
			if (arguments.length < 7) {
				executor = callback;
				callback = readOnly;
				readOnly = response;
				response = request;
				request = data;
				data = null;
			}

			// readOnly argument is optional
			if (arguments.length < 6) {
				executor = callback;
				callback = readOnly;
				readOnly = false;
			}

            var stackTrace = new Error();
			var config = this.config;
			var logger = this.logger;
            var startTime;
            var clientId;
            if (this.config && this.config.logLevel !== undefined){
                startTime = Date.now();
                if (request) {
                    request.query = query;
                }
            }

            logger.trace('Postgres QUERY=>', query, data);
			this.getConnection(function (error, client, done) {
				if (error) {
					return callback(error);
				}
				if (config.logLevel && config.logLevel > config.logLevels.NONE) {
                    clientId = client.id;
                    var tzoffset = (new Date()).getTimezoneOffset() * 60000;
                    var localISOTime = (new Date(Date.now() - tzoffset)).toISOString().slice(0, -1);
                    logger.info(localISOTime+' - connection id: '+client.id+ ' - '+query+' effective_user: ('+client.effective_user_id+')');
                }
				client.query(query, data, function (error, results) {
					try {
                        logger.trace('connection released back to the pool');
						done();
					} catch (E) { }

					if (error) {
                        console.error('Postgres query failure: '+ error+'; '+stackTrace.stack);
						callback(error);
					} else {
                        if (config.logLevel && config.logLevel > config.logLevels.NONE) {
                            var tzoffset = (new Date()).getTimezoneOffset() * 60000;
                            var localISOTime = (new Date(Date.now() - tzoffset)).toISOString().slice(0, -1);

                            if (config.logLevel === config.logLevels.TRACE) {
                                logger.info(localISOTime + ' - connection id: '+clientId+' - '+query, 'result count:', results.rows.length, data);
                                logger.info(localISOTime + ' - connection id: '+clientId+' - results:', results);
                                logger.info(localISOTime + ' - connection id: '+clientId+' - query response time: ' + (Date.now() - startTime) + 'ms');
                            }
                            if (config.logLevel === config.logLevels.DEBUG) {
                                logger.info(localISOTime + ' - connection id: '+clientId+' - '+query, 'result count:', results.rows.length, data);
                                logger.info(localISOTime + ' - connection id: '+clientId+' - query response time: ' + (Date.now() - startTime) + 'ms');
                            } else {
                                logger.info(localISOTime + ' - connection id: '+clientId+' - query response time: ' + (Date.now() - startTime) + 'ms');
                            }
                        }

                        logger.trace(results);
						executor(results);
					}
				});
			}, request, response, readOnly);
		},

		/**
		 * The request object will be on the model if model.createRequest was called, or it will be attached to the
		 * connector for autogenerated CRUD APIs
		 */
		_getRequest: function _getRequest(Model) {
			return Model.request || this.request;
		},

		/**
		 * The response object will be on the model if model.createRequest was called, or it will be attached to the
		 * connector for autogenerated CRUD APIs
		 */
		_getResponse: function _getResponse(Model) {
			return Model.response || this.response;
		}

	});

};

function allocateConnection(config, request, response, logger, callback, error, connection, done) {
    // support for tracking connections by id
    if (connection.id === undefined) {
        connection.id = Date.now();
    }

    if (error) {
		callback(error);
	} else if (config.onAllocateConnection) {
		config.onAllocateConnection(
			connection,
			request,
			response,
			config,
			logger,
			function afterConnectionAllocationCallback() {
				callback(null, connection, deallocateConnection.bind(
					null,
					config,
					request,
					response,
					logger,
					connection,
					done
				));
			}
		);
	} else {
		callback(null, connection, deallocateConnection.bind(null, config, request, response, logger, connection, done));
	}
}

function deallocateConnection(config, request, response, logger, connection, done) {
	if (config.onDeallocateConnection) {
		config.onDeallocateConnection(connection, request, response, config, logger, done);
	} else {
		done();
	}
}

/**
 * Prefix table name with schema name if the table is not in the search path
 */
function namespaceTable(table_name, schema_name, searchPath) {
	return (!searchPath[schema_name] ? schema_name + '.' : '') + table_name;
}

/**
 * Get number of available connections in Postgres pool
 *
 * @param {Object} pool Pool to check
 * @param {Number} Number of available connections
 */
function poolAvailableConnections(pool) {
	return pool.pool.availableObjectsCount();
}

/**
 * Get remaining space in pool for new connections
 *
 * @param {Object} pool Pool to check
 * @param {Number} Space for new connections
 */
function poolAvailableSpace(pool) {
	return pool.pool.getMaxPoolSize() - pool.pool.getPoolSize();
}
