# mysql-events
![CircleCI](https://circleci.com/gh/rodrigogs/mysql-events.svg)
[![Code Climate](https://codeclimate.com/github/rodrigogs/mysql-events/badges/gpa.svg)](https://codeclimate.com/github/rodrigogs/mysql-events)
[![Test Coverage](https://codeclimate.com/github/rodrigogs/mysql-events/badges/coverage.svg)](https://codeclimate.com/github/rodrigogs/mysql-events/coverage)

A [node.js](https://nodejs.org) package that watches a MySQL database and runs callbacks on matched events.

This package is based on the [ZongJi](https://github.com/nevill/zongji) node module. Please make sure that you meet the requirements described at [ZongJi](https://github.com/nevill/zongji), like MySQL binlog etc.

# Quick Start
```javascript
// Inside an async function

const MySQLEvents = require('@rodrigogs/mysql-events');

const instance = new MySQLEvents({
  host: 'localhost',
  user: 'root',
  password: 'root',
}, {
  startAtEnd: true,
  includeSchema: {
    MY_SCHEMA: [
      'MY_TABLE',
    ],
  },
  excludeSchema: {
    mysql: true,
  },
});

await instance.start();

instance.addTrigger({
  expression: 'SCHEMA.TABLE.column',
  statement: MySQLEvents.STATEMENTS.ALL,
  name: 'TEST',
  callback: (event) => {
    console.log(event);
  },
});

instance.on(MySQLEvents.EVENTS.CONNECTION_ERROR, console.error);
instance.on(MySQLEvents.EVENTS.ZONGJI_ERROR, console.error);
```

# Install
```sh
npm install @rodrigogs/mysql-events
```

# Usage
- Instantiate and create a database connection using a DSN
```javascript
const dsn = {
  host: 'localhost',
  user: 'username',
  password: 'password',
};

const myInstance = new MySQLEvents(dsn, { /* zongji options */ });
```

- Instantiate and create a database connection using a preexisting connection
```javascript
const connection = mysql.createConnection({
  host: 'localhost',
  user: 'username',
  password: 'password',
});

const myInstance = new MySQLEvents(connection, { /* zongji options */ });
```

Make sure the database user has the privilege to read the binlog on database that you want to watch on.

This will listen to any change in the _fieldName_ and if the changed value is equal to __Active__, then triggers the callback. Passing it 2 arguments. Argument value depends on the event.

- Insert: before = null, after = rowObject
- Update: before = rowObject, after = rowObject
- Delete: before = rowObject, after = null

### `rowObject`
It has the following structure:

```
{
  before: {...columns},
  after: {...columns},
}
```

## Remove a trigger
```javascript
instance.removeTrigger({
  expression: 'schema.table',
  statement: 'INSERT',
  name: 'trigger_name',
});
```

## Stop all events on the connection
```javascript
await instance.stop();
```

## Additional options
You can find the list of the available options [here](https://github.com/nevill/zongji#zongji-class).

# Watcher Setup
Its basically a dot '.' seperated string. It can have the following combinations.

- *schema*: watches the whole database for changes (insert/update/delete). Which table and row are affected can be inspected from the oldRow & newRow
  - If '*', it will watch changes for any database schema.
- *schema.table*: watches the whole table for changes. Which rows are affected can be inspected from the oldRow & newRow
  - If '*' or '', will watch for any change in schema(s) tables.
- *schema.table.column*: watches for changes in the column. Which database, table & other changed columns can be inspected from the oldRow & newRow
  - If '*' or '', will watch for any change in schema(s) tables(s) columns.

### LICENSE
[BSD-3-Clause](https://github.com/rodrigogs/mysql-events/blob/master/LICENSE) © Rodrigo Gomes da Silva
