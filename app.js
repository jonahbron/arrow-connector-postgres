/**
 * NOTE: This file is simply for testing this connector and will not
 * be used or packaged with the actual connector when published.
 */
var Arrow = require('arrow'),
	server = new Arrow();

var Post = Arrow.Model.extend('post', {
	fields: {
		title: { type: String },
		content: { type: String }
	},
	connector: 'postgres'
});
server.addModel(Post);

server.start(function(err) {
	if (err) { server.logger.fatal(err); }
	server.logger.info('server started on port', server.port);

	// Create some posts programmatically.
	var posts = [
		{ title: 'A journey through time', content: 'take a journey' },
		{ title: 'The Bible', content: 'In the beginning, God created the heavens and the earth.' }
	];
	Post.create(posts, function(err, posts) {
		if (err) { return server.logger.error(err); }
		server.logger.info('Created some posts:', [ posts[0], posts[1] ]);

		Post.find({ title: 'The Bible' }, function(err, results) {
			server.logger.info('Found The Bible:', results && results[0]);
		});
	});

});