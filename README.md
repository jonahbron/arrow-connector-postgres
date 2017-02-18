# Postgres Connector

This is an Arrow connector to Postgres.

## Installation

```bash
$ appc install connector/postgres --save
```

A configuration file is generated for you. By default we use a host of `localhost`, a user of `root` and an
empty password to connect. However, you must set a database in your configuration.

## Usage

The Postgres connector will automatically generate models from the tables in your database.  You will not normally need to define them yourself.

To reference a model, use `Arrow.getModel`

```javascript
var Arrow = require('arrow');
var User = Arrow.getModel('postgres/users');
```

If you need to explicitly specify the schema, you can do so in the model name.

```javascript
var User = Arrow.getModel('postgres/my_schema.users');
```

You can only reference schemas whitelisted in the `schemas` config option.  Do not specify a schema name if your schema is already in the [`search_path`](https://www.postgresql.org/docs/9.3/static/ddl-schemas.html#DDL-SCHEMAS-PATH).

## Defining Models

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

## Development

> This section is for individuals developing the Postgres Connector and not intended
  for end-users.

```bash
npm install
node app.js
```

### Running Unit Tests

To use the tests, you'll want to create a database with the following tables:

```
CREATE TABLE post
(
	id SERIAL PRIMARY KEY,
	title VARCHAR(255),
	content VARCHAR(255)
);
CREATE TABLE super_post
(
	id SERIAL PRIMARY KEY,
	title VARCHAR(255),
	content VARCHAR(255)
);
CREATE TABLE employee
(
	id SERIAL PRIMARY KEY,
	first_name VARCHAR(255),
	last_name VARCHAR(255),
	email_address VARCHAR(255)
);
```

```bash
npm test
```


# Contributing

This project is open source and licensed under the [Apache Public License (version 2)](http://www.apache.org/licenses/LICENSE-2.0).  Please consider forking this project to improve, enhance or fix issues. If you feel like the community will benefit from your fork, please open a pull request.


# Legal Stuff

Appcelerator is a registered trademark of Appcelerator, Inc. Arrow and associated marks are trademarks of Appcelerator. All other marks are intellectual property of their respective owners. Please see the LEGAL information about using our trademarks, privacy policy, terms of usage and other legal information at [http://www.appcelerator.com/legal](http://www.appcelerator.com/legal).
