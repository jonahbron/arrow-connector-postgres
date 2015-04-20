var should = require('should'),
	async = require('async'),
	_ = require('lodash'),
	Arrow = require('arrow'),
	server = new Arrow(),
	log = Arrow.createLogger({}, { name: 'postgres TEST', useConsole: true, level: 'info' }),
	connector = server.getConnector('postgres'),
	Model;

describe('Connector', function() {

	before(function(next) {
		// define your model
		Model = Arrow.Model.extend('post', {
			fields: {
				title: { type: String },
				content: { type: String }
			},
			connector: 'postgres'
		});

		should(Model).be.an.object;

		Model.deleteAll(function() {
			next();
		});
	});

	afterEach(function(next) {
		Model.deleteAll(function(err) {
			if (err) {
				log.error(err.message);
			}
			next();
		});
	});

	it('should be able to fetch schema', function(next) {
		connector.fetchSchema(function(err, schema) {
			should(err).be.not.ok;
			should(schema).be.an.object;
			next();
		});
	});

	it('should create models from tables', function() {
		var SuperPost = connector.getModel('postgres/super_post');
		should(SuperPost).be.ok;
		should(SuperPost.generated).be.true;
	});

	it('should be able to extend from tables', function(next) {
		Arrow.getConnector('postgres').enabled = true;
		var SupererPost = Arrow.Model.extend('postgres/super_post', 'superer_post', {
			fields: {
				MyTitle: { name: 'title', type: String },
				MyContent: { name: 'content', type: String }
			},
			connector: 'postgres'
		});
		should(SupererPost).be.ok;
		should(SupererPost._supermodel).be.ok;
		should(SupererPost._parent).be.ok;
		SupererPost.create({
			MyTitle: 'cheese',
			MyContent: 'was eaten this night'
		}, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.Object;
			should(instance.getPrimaryKey()).be.a.Number;
			should(instance.MyTitle).be.ok;
			should(instance.MyContent).be.ok;
			next();
		});
	});

	it('should be able to use named fields', function(next) {
		var Model = Arrow.Model.extend('post', {
				fields: {
					MyTitle: { name: 'title', type: String },
					MyContent: { name: 'content', type: String }
				},
				connector: 'postgres'
			}),
			title = 'Test',
			content = 'Hello world',
			object = {
				MyTitle: title,
				MyContent: content
			};
		Model.create(object, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.Object;
			should(instance.getPrimaryKey()).be.a.Number;
			should(instance.MyTitle).equal(title);
			should(instance.MyContent).equal(content);
			Model.query({
				where: {
					MyTitle: 'Test'
				}
			}, function(err, coll) {
				should(err).be.not.ok;
				should(coll.length).be.greaterThan(0);
				instance.delete(next);
			});
		});
	});

	it('API-298: should be able to use named fields', function(next) {
		// create a model from a postgres table
		var uc_1 = Arrow.createModel('uc_1', {
				fields: {
					fname: { type: String, description: 'First name', name: 'first_name', required: true },
					lname: { type: String, description: 'Last name', required: true, name: 'last_name' },
					email: { type: String, description: 'Email address', required: true, name: 'email_address' }
				},
				connector: 'postgres',
				metadata: {
					'postgres': { table: 'employee' }
				}
			}),
			object = {
				fname: 'Test',
				lname: 'Smith',
				email: 'dtoth@appcelerator.com'
			};
		uc_1.create(object, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.Object;
			should(instance.getPrimaryKey()).be.a.Number;
			should(instance.fname).equal(object.fname);
			should(instance.lname).equal(object.lname);
			should(instance.email).equal(object.email);
			uc_1.query({
				where: {
					lname: object.lname
				}
			}, function(err, coll) {
				should(err).be.not.ok;
				should(coll.length).be.greaterThan(0);
				instance.delete(next);
			});
		});
	});

	it('should be able to ignore fields absent from schema', function(next) {
		var Model = Arrow.Model.extend('post', {
				fields: {
					MyTitle: { name: 'title', type: String },
					MyContent: { name: 'content', type: String },
					MyCustomField: { type: String }
				},
				connector: 'postgres'
			}),
			title = 'Test',
			content = 'Hello world',
			object = {
				MyTitle: title,
				MyContent: content,
				MyCustomField: 'should be ignored'
			};
		Model.create(object, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.Object;
			should(instance.getPrimaryKey()).be.a.Number;
			should(instance.MyTitle).equal(title);
			should(instance.MyContent).equal(content);
			instance.delete(next);
		});
	});

	it('should be able to create instance', function(next) {

		var title = 'Test',
			content = 'Hello world',
			object = {
				title: title,
				content: content
			};

		Model.create(object, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.object;
			should(instance.getPrimaryKey()).be.a.Number;
			should(instance.title).equal(title);
			should(instance.content).equal(content);
			instance.delete(next);
		});

	});

	it('should be able to fetch schema with post table', function(next) {
		connector.fetchSchema(function(err, schema) {
			should(err).be.not.ok;
			should(schema).be.an.object;
			should(schema.objects.post.id).be.ok;
			should(schema.objects.post.title).be.ok;
			should(schema.objects.post.content).be.ok;
			should(schema.primary_keys.post).be.ok;
			next();
		});
	});

	it('should be able to find an instance by ID', function(next) {

		var title = 'Test',
			content = 'Hello world',
			object = {
				title: title,
				content: content
			};

		Model.create(object, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.object;

			var id = instance.getPrimaryKey();
			Model.find(id, function(err, instance2) {
				should(err).be.not.ok;
				should(instance2).be.an.object;
				should(instance2.getPrimaryKey()).equal(id);
				should(instance2.title).equal(title);
				should(instance2.content).equal(content);
				instance.delete(next);
			});

		});

	});

	it('should be able to find an instance by field value', function(next) {

		var title = 'Test',
			content = 'Hello world',
			object = {
				title: title,
				content: content
			};

		Model.create(object, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.object;

			var query = { title: title };
			Model.find(query, function(err, coll) {
				should(err).be.not.ok;
				var instance2 = coll[0];
				should(instance2).be.an.object;
				should(instance2.title).equal(title);
				instance.delete(next);
			});

		});

	});

	it('should be able to query', function(callback) {

		var title = 'Test',
			content = 'Hello world',
			object = {
				title: title,
				content: content
			};

		Model.create(object, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.object;

			var options = {
				where: { content: { $like: 'Hello%' } },
				sel: { content: 1 },
				order: { title: -1, content: 1 },
				limit: 3,
				skip: 0
			};
			Model.query(options, function(err, coll) {
				should(err).be.not.ok;

				async.eachSeries(coll, function(obj, next) {
					should(obj.getPrimaryKey()).be.a.Number;
					should(obj.title).be.not.ok;
					should(obj.content).be.a.string;
					obj.remove(next);
				}, callback);
			});
		});

	});

	it('should be able to query in list', function(callback) {

		var title = 'Test',
			content = 'Hello world',
			object = {
				title: title,
				content: content
			};

		Model.create(object, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.object;

			var options = {
				where: { content: { $in: ['Foo', 'Test'] } },
				sel: { content: 1 },
				order: { title: -1, content: 1 },
				limit: 3,
				skip: 0
			};
			Model.query(options, function(err, coll) {
				should(err).be.not.ok;

				async.eachSeries(coll, function(obj, next) {
					should(obj.getPrimaryKey()).be.a.Number;
					should(obj.title).be.not.ok;
					should(obj.content).be.a.string;
					obj.remove(next);
				}, callback);
			});
		});

	});

	it('should not have false positives in an IN query', function(callback) {

		var title = 'Test',
			content = 'Hello world',
			object = {
				title: title,
				content: content
			};

		Model.create(object, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.object;

			var options = {
				where: { content: { $in: ['Foo', 'Bar'] } },
				sel: { content: 1 },
				order: { title: -1, content: 1 },
				limit: 3,
				skip: 0
			};
			Model.query(options, function(err, coll) {
				should(err).be.not.ok;
				coll.length.should.be.zero;
				callback();
			});
		});

	});

	it('should support greater-than queries', function(callback) {
		var object1 = {
				title: '1',
				content: 'baz'
			},
			object2 = {
				title: '3',
				content: 'baz',
			};

		Model.create(object1, function(err, instance1) {
			Model.create(object2, function(err, instance2) {
				should(err).be.not.ok;
				should(instance1).be.an.object;
				should(instance2).be.an.object;

				var options = {
					where: { title: { $gt: '2' } }
				};
				Model.query(options, function(err, coll) {
					should(err).be.not.ok;
					coll.length.should.equal(1);
					coll[0].title.should.equal('3');
					callback();
				});
			});
		});
	});

	it('should support greater-than or equal queries', function(callback) {
		var object1 = {
				title: '1',
				content: 'baz'
			},
			object2 = {
				title: '3',
				content: 'baz',
			};

		Model.create(object1, function(err, instance1) {
			Model.create(object2, function(err, instance2) {
				should(err).be.not.ok;
				should(instance1).be.an.object;
				should(instance2).be.an.object;

				var options = {
					where: { title: { $gte: '3' } }
				};
				Model.query(options, function(err, coll) {
					should(err).be.not.ok;
					coll.length.should.equal(1);
					coll[0].title.should.equal('3');
					callback();
				});
			});
		});
	});

	it('should support less-than queries', function(callback) {
		var object1 = {
				title: '1',
				content: 'baz'
			},
			object2 = {
				title: '3',
				content: 'baz',
			};

		Model.create(object1, function(err, instance1) {
			Model.create(object2, function(err, instance2) {
				should(err).be.not.ok;
				should(instance1).be.an.object;
				should(instance2).be.an.object;

				var options = {
					where: { title: { $lt: '2' } }
				};
				Model.query(options, function(err, coll) {
					should(err).be.not.ok;
					coll.length.should.equal(1);
					coll[0].title.should.equal('1');
					callback();
				});
			});
		});
	});

	it('should support less-than or equal queries', function(callback) {
		var object1 = {
				title: '1',
				content: 'baz'
			},
			object2 = {
				title: '3',
				content: 'baz',
			};

		Model.create(object1, function(err, instance1) {
			Model.create(object2, function(err, instance2) {
				should(err).be.not.ok;
				should(instance1).be.an.object;
				should(instance2).be.an.object;

				var options = {
					where: { title: { $lte: '1' } }
				};
				Model.query(options, function(err, coll) {
					should(err).be.not.ok;
					coll.length.should.equal(1);
					coll[0].title.should.equal('1');
					callback();
				});
			});
		});
	});

	it('should handle NULL checks properly', function(callback) {
		var object = {
			title: '1',
			content: null
		};

		Model.create(object, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.object;

			var options = {
				where: { content: null }
			};
			Model.query(options, function(err, coll) {
				should(err).be.not.ok;
				coll.length.should.equal(1);
				coll[0].title.should.equal('1');
				callback();
			});
		});
	});

	it('API-325: should be able to query with unsel', function(callback) {

		var Model = Arrow.Model.extend('post', {
			fields: {
				myTitle: { name: 'title', type: String },
				myContent: { name: 'content', type: String }
			},
			connector: 'postgres'
		});
		var title = 'Test',
			content = 'Hello world',
			object = {
				myTitle: title,
				myContent: content
			};

		Model.create(object, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.object;

			var options = {
				where: { myContent: { $like: 'Hello%' } },
				unsel: { myTitle: 1 },
				order: { myTitle: -1, myContent: 1 },
				limit: 3,
				skip: 0
			};
			Model.query(options, function(err, coll) {
				should(err).be.not.ok;

				async.eachSeries(coll, function(obj, next) {
					should(obj.getPrimaryKey()).be.a.Number;
					should(obj.myTitle).be.not.ok;
					should(obj.myContent).be.a.String;
					obj.remove(next);
				}, callback);
			});
		});

	});

	it('should be able to find all instances', function(next) {

		var posts = [
			{
				title: 'Test1',
				content: 'Hello world'
			},
			{
				title: 'Test2',
				content: 'Goodbye world'
			}];

		Model.deleteAll(function() {
			Model.create(posts, function(err, coll) {
				var keys = [];
				coll.forEach(function(post) {
					keys.push(post.getPrimaryKey());
				});

				Model.find(function(err, coll2) {
					should(err).be.not.ok;
					should(coll2.length).equal(coll.length);

					coll2.forEach(function(post, i) {
						should(post.getPrimaryKey()).equal(keys[i]);
					});

					Model.deleteAll(next);
				});

			});
		});

	});

	it('API-337: should be able to query with order', function(next) {

		var posts = [
			{
				title: 'Test1',
				content: 'Hello world'
			},
			{
				title: 'Test2',
				content: 'Goodbye world'
			},
			{
				title: 'Test3',
				content: 'Goodbye world'
			}
		];

		Model.deleteAll(function() {
			Model.create(posts, function(err, coll) {
				should(err).be.not.ok;

				Model.query({ order: { title: 1 }, limit: 2 }, function(err, coll2) {
					should(err).be.not.ok;
					should(coll2[0].getPrimaryKey()).equal(coll[0].getPrimaryKey());
					should(coll2[1].getPrimaryKey()).equal(coll[1].getPrimaryKey());

					Model.query({ order: { title: -1 } }, function(err, coll2) {
						should(err).be.not.ok;
						should(coll2[0].getPrimaryKey()).equal(coll[2].getPrimaryKey());
						should(coll2[1].getPrimaryKey()).equal(coll[1].getPrimaryKey());

						Model.deleteAll(next);
					});
				});

			});
		});

	});

	it('API-281: should support paging', function(next) {

		var posts = [];
		for (var i = 1; i <= 30; i++) {
			posts.push({
				title: 'Test' + i,
				content: 'Hello world'
			});
		}

		Model.deleteAll(function() {
			Model.create(posts, function(err, coll) {
				should(err).be.not.ok;
				should(coll).be.an.object;

				Model.query({ per_page: 1, page: 2 }, function(err, coll2) {
					should(err).be.not.ok;
					should(coll2.getPrimaryKey()).equal(coll[1].getPrimaryKey());

					Model.query({ skip: 1, limit: 1 }, function(err, coll2) {
						should(err).be.not.ok;
						should(coll2.getPrimaryKey()).equal(coll[1].getPrimaryKey());

						Model.query({ page: 2 }, function(err, coll2) {
							should(err).be.not.ok;
							should(coll2[0].getPrimaryKey()).equal(coll[10].getPrimaryKey());

							Model.deleteAll(next);
						});
					});
				});

			});
		});

	});

	it('should be able to update an instance', function(next) {

		var title = 'Test',
			content = 'Hello world',
			object = {
				title: title,
				content: content
			};

		Model.create(object, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.object;

			var id = instance.getPrimaryKey();
			Model.find(id, function(err, instance2) {
				should(err).be.not.ok;

				instance2.set('content', 'Goodbye world');
				instance2.save(function(err, result) {
					should(err).be.not.ok;

					should(result).be.an.object;
					should(result.getPrimaryKey()).equal(id);
					should(result.title).equal(title);
					should(result.content).equal('Goodbye world');
					instance.delete(next);
				});

			});

		});

	});
	
	it('API-377: should be able to query with just skip', function(callback) {
		Model.query({}, function(err, coll1) {
			should(err).be.not.ok;
			should(coll1).be.ok;
			Model.query({ skip: 1 }, function(err, coll2) {
				should(err).be.not.ok;
				should(coll2).be.ok;
				callback();
			});
		});
	});

	it('Should allow passing arbitrary SQL to WHERE clause with prepared statements', function(callback) {

		var title = 'Test',
			content = 'Hello world',
			object = {
				title: title,
				content: content
			};

		Model.create(object, function(err, instance) {
			should(err).be.not.ok;
			should(instance).be.an.object;

			var options = {
				where: { '': { $sql: 'title = $1 AND content != $1', values: ['Test'] } },
				sel: { content: 1 },
				order: { title: -1, content: 1 },
				limit: 3,
				skip: 0
			};
			Model.query(options, function(err, coll) {
				should(err).be.not.ok;
				coll.length.should.equal(1);
				callback();
			});
		});

	});

});
