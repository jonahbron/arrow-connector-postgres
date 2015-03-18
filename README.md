# Postgres Connector

This is an Arrow connector to Postgres.

> This software is pre-release and not yet ready for usage.  Please don't use this just yet while we're working through testing and finishing it up. Once it's ready, we'll make an announcement about it.

To install:

```bash
$ appc install connector/postgres --save
```

By default we use `localhost`, `root` and empty password.

However, you must set a database in your configuration.

Now reference the connector in your model.

```javascript
var Account = Arrow.Model.extend('Account',{
	fields: {
		Name: { type: String, required: true, validator: /[a-zA-Z]{3,}/ }
	},
	connector: 'postgres'
});
```

If you want to map a specific model to a specific table, use metadata.  For example, to map the `account` model to the table named `accounts`, set it such as:

```javascript
var Account = Arrow.Model.extend('account',{
	fields: {
		Name: { type: String, required: false, validator: /[a-zA-Z]{3,}/ }
	},
	connector: 'postgres',
	metadata: {
		'postgres': {
			table: 'accounts'
		}
	}
});
```

# Testing

To use the tests, you'll want to create a database with the following tables:

```
CREATE TABLE post
(
	id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	title VARCHAR(255),
	content VARCHAR(255)
);
CREATE TABLE super_post
(
	id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	title VARCHAR(255),
	content VARCHAR(255)
);
CREATE TABLE employee
(
	id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	first_name VARCHAR(255),
	last_name VARCHAR(255),
	email_address VARCHAR(255)
);
```

# Contributing

This project is open source and licensed under the [Apache Public License (version 2)](http://www.apache.org/licenses/LICENSE-2.0).  Please consider forking this project to improve, enhance or fix issues. If you feel like the community will benefit from your fork, please open a pull request.


# Legal Stuff

Appcelerator is a registered trademark of Appcelerator, Inc. Arrow and associated marks are trademarks of Appcelerator. All other marks are intellectual property of their respective owners. Please see the LEGAL information about using our trademarks, privacy policy, terms of usage and other legal information at [http://www.appcelerator.com/legal](http://www.appcelerator.com/legal).
