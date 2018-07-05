/* eslint-disable padded-blocks,no-unused-expressions */

const chai = require('chai');
const mysql = require('mysql');
const MySQLEvents = require('./lib');

const { expect } = chai;

const TEST_SCHEMA = 'testSchema';
const TEST_TABLE = 'testTable';
const TEST_COLUMN_1 = 'column1';
const TEST_COLUMN_2 = 'column2';

const delay = (timeout = 500) => new Promise((resolve) => {
  setTimeout(resolve, timeout);
});

const getConnection = () => {
  const connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'root',
  });

  return new Promise((resolve, reject) => connection.connect((err) => {
    if (err) return reject(err);
    resolve(connection);
  }));
};

const executeQuery = (conn, query) => {
  return new Promise((resolve, reject) => conn.query(query, (err, results) => {
    if (err) return reject(err);
    resolve(results);
  }));
};

const closeConnection = conn => new Promise((resolve, reject) => conn.end((err) => {
  if (err) return reject(err);
  resolve();
}));

const grantPrivileges = async () => {
  const conn = await getConnection();
  try {
    await executeQuery(conn, 'GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO \'root\'@\'localhost\'');
  } catch (err) {
    throw err;
  } finally {
    await closeConnection(conn);
  }
};

const createSchema = async () => {
  const conn = await getConnection();
  try {
    await executeQuery(conn, `CREATE DATABASE ${TEST_SCHEMA};`);
  } catch (err) {
    throw err;
  } finally {
    await closeConnection(conn);
  }
};

const dropSchema = async () => {
  const conn = await getConnection();
  try {
    await executeQuery(conn, `DROP DATABASE ${TEST_SCHEMA};`);
  } catch (err) {
    throw err;
  } finally {
    await closeConnection(conn);
  }
};

const createTables = async () => {
  const conn = await getConnection();
  try {
    await executeQuery(conn, `USE ${TEST_SCHEMA};`);
    await executeQuery(conn, `CREATE TABLE ${TEST_TABLE} (${TEST_COLUMN_1} varchar(255), ${TEST_COLUMN_2} varchar(255));`);
  } catch (err) {
    throw err;
  } finally {
    await closeConnection(conn);
  }
};

const dropTables = async () => {
  const conn = await getConnection();
  try {
    await executeQuery(conn, `USE ${TEST_SCHEMA};`);
    await executeQuery(conn, `DROP TABLE ${TEST_TABLE};`);
  } catch (err) {
    throw err;
  } finally {
    await closeConnection(conn);
  }
};

before(async () => {
  chai.should();
  await createSchema();
  await grantPrivileges();
});

beforeEach(async () => {
  await createTables();
});

afterEach(async () => {
  await dropTables();
});

after(async () => {
  await dropSchema();
});

describe('MySQLEvents', () => {

  it('should connect and disconnect from MySQL using a pre existing connection', async () => {
    const connection = mysql.createConnection({
      host: 'localhost',
      user: 'root',
      password: 'root',
    });

    const instance = new MySQLEvents(connection);

    await instance.start();

    await delay();

    await instance.stop();
  }).timeout(10000);

  it('should connect and disconnect from MySQL using a dsn', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    });

    await instance.start();

    await delay();

    await instance.stop();
  }).timeout(10000);

  it('should connect and disconnect from MySQL using a connection string', async () => {
    const instance = new MySQLEvents(`mysql://root:root@localhost/${TEST_SCHEMA}`);

    await instance.start();

    await delay();

    await instance.stop();
  }).timeout(10000);

  it('should catch an event through an INSERT trigger', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    }, {
      startAtEnd: true,
      excludedSchemas: {
        mysql: true,
      },
    });

    await instance.start();

    let triggerEvent = null;
    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA}.${TEST_TABLE}`,
      statement: MySQLEvents.STATEMENTS.INSERT,
      callback: (event) => {
        triggerEvent = event;
      },
    });

    await delay(1000);

    await executeQuery(instance.connection, `USE ${TEST_SCHEMA};`);
    await executeQuery(instance.connection, `INSERT INTO ${TEST_TABLE} VALUES ('test1', 'test2');`);

    await delay(1000);

    if (!triggerEvent) throw new Error('No trigger was caught');

    triggerEvent.should.be.an('object');

    triggerEvent.should.have.ownPropertyDescriptor('type');
    triggerEvent.type.should.be.a('string').equals('INSERT');

    triggerEvent.should.have.ownPropertyDescriptor('timestamp');
    triggerEvent.timestamp.should.be.a('number');

    triggerEvent.should.have.ownPropertyDescriptor('table');
    triggerEvent.table.should.be.a('string').equals(TEST_TABLE);

    triggerEvent.should.have.ownPropertyDescriptor('schema');
    triggerEvent.schema.should.be.a('string').equals(TEST_SCHEMA);

    triggerEvent.should.have.ownPropertyDescriptor('nextPosition');
    triggerEvent.nextPosition.should.be.a('number');

    triggerEvent.should.have.ownPropertyDescriptor('affectedRows');
    triggerEvent.affectedRows.should.be.an('array').to.have.lengthOf(1);
    triggerEvent.affectedRows[0].should.be.an('object');
    triggerEvent.affectedRows[0].should.have.ownPropertyDescriptor('after');
    triggerEvent.affectedRows[0].after.should.be.an('object');
    triggerEvent.affectedRows[0].after.should.have.ownPropertyDescriptor(TEST_COLUMN_1);
    triggerEvent.affectedRows[0].after[TEST_COLUMN_1].should.be.a('string').equals('test1');
    triggerEvent.affectedRows[0].after.should.have.ownPropertyDescriptor(TEST_COLUMN_2);
    triggerEvent.affectedRows[0].after[TEST_COLUMN_2].should.be.a('string').equals('test2');
    triggerEvent.affectedRows[0].should.have.ownPropertyDescriptor('before');
    expect(triggerEvent.affectedRows[0].before).to.be.an('undefined');

    await instance.stop();
  }).timeout(10000);

  it('should catch an event through an UPDATE trigger', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    }, {
      startAtEnd: true,
      excludedSchemas: {
        mysql: true,
      },
    });

    await instance.start();

    let triggerEvent = null;
    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA}.${TEST_TABLE}`,
      statement: MySQLEvents.STATEMENTS.UPDATE,
      callback: (event) => {
        triggerEvent = event;
      },
    });

    await delay(1000);

    await executeQuery(instance.connection, `USE ${TEST_SCHEMA};`);
    await executeQuery(instance.connection, `INSERT INTO ${TEST_TABLE} VALUES ('test1', 'test2');`);
    await executeQuery(instance.connection, `UPDATE ${TEST_TABLE} SET ${TEST_COLUMN_1} = 'test3', ${TEST_COLUMN_2} = 'test4';`);

    await delay(1000);

    if (!triggerEvent) throw new Error('No trigger was caught');

    triggerEvent.should.be.an('object');

    triggerEvent.should.have.ownPropertyDescriptor('type');
    triggerEvent.type.should.be.a('string').equals('UPDATE');

    triggerEvent.should.have.ownPropertyDescriptor('timestamp');
    triggerEvent.timestamp.should.be.a('number');

    triggerEvent.should.have.ownPropertyDescriptor('table');
    triggerEvent.table.should.be.a('string').equals(TEST_TABLE);

    triggerEvent.should.have.ownPropertyDescriptor('schema');
    triggerEvent.schema.should.be.a('string').equals(TEST_SCHEMA);

    triggerEvent.should.have.ownPropertyDescriptor('nextPosition');
    triggerEvent.nextPosition.should.be.a('number');

    triggerEvent.should.have.ownPropertyDescriptor('affectedRows');
    triggerEvent.affectedRows.should.be.an('array').to.have.lengthOf(1);
    triggerEvent.affectedRows[0].should.be.an('object');

    triggerEvent.affectedRows[0].should.have.ownPropertyDescriptor('after');
    triggerEvent.affectedRows[0].after.should.be.an('object');
    triggerEvent.affectedRows[0].after.should.have.ownPropertyDescriptor(TEST_COLUMN_1);
    triggerEvent.affectedRows[0].after[TEST_COLUMN_1].should.be.a('string').equals('test3');
    triggerEvent.affectedRows[0].after.should.have.ownPropertyDescriptor(TEST_COLUMN_2);
    triggerEvent.affectedRows[0].after[TEST_COLUMN_2].should.be.a('string').equals('test4');

    triggerEvent.affectedRows[0].should.have.ownPropertyDescriptor('before');
    triggerEvent.affectedRows[0].before.should.be.an('object');
    triggerEvent.affectedRows[0].before.should.have.ownPropertyDescriptor(TEST_COLUMN_1);
    triggerEvent.affectedRows[0].before[TEST_COLUMN_1].should.be.a('string').equals('test1');
    triggerEvent.affectedRows[0].before.should.have.ownPropertyDescriptor(TEST_COLUMN_2);
    triggerEvent.affectedRows[0].before[TEST_COLUMN_2].should.be.a('string').equals('test2');

    await instance.stop();
  }).timeout(10000);

  it('should catch an event through an DELETE trigger', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    }, {
      startAtEnd: true,
      excludedSchemas: {
        mysql: true,
      },
    });

    await instance.start();

    let triggerEvent = null;
    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA}.${TEST_TABLE}`,
      statement: MySQLEvents.STATEMENTS.DELETE,
      callback: (event) => {
        triggerEvent = event;
      },
    });

    await delay(1000);

    await executeQuery(instance.connection, `USE ${TEST_SCHEMA};`);
    await executeQuery(instance.connection, `INSERT INTO ${TEST_TABLE} VALUES ('test1', 'test2');`);
    await executeQuery(instance.connection, `DELETE FROM ${TEST_TABLE} WHERE ${TEST_COLUMN_1} = 'test1' AND ${TEST_COLUMN_2} = 'test2';`);

    await delay(1000);

    if (!triggerEvent) throw new Error('No trigger was caught');

    triggerEvent.should.be.an('object');

    triggerEvent.should.have.ownPropertyDescriptor('type');
    triggerEvent.type.should.be.a('string').equals('DELETE');

    triggerEvent.should.have.ownPropertyDescriptor('timestamp');
    triggerEvent.timestamp.should.be.a('number');

    triggerEvent.should.have.ownPropertyDescriptor('table');
    triggerEvent.table.should.be.a('string').equals(TEST_TABLE);

    triggerEvent.should.have.ownPropertyDescriptor('schema');
    triggerEvent.schema.should.be.a('string').equals(TEST_SCHEMA);

    triggerEvent.should.have.ownPropertyDescriptor('nextPosition');
    triggerEvent.nextPosition.should.be.a('number');

    triggerEvent.should.have.ownPropertyDescriptor('affectedRows');
    triggerEvent.affectedRows.should.be.an('array').to.have.lengthOf(1);
    triggerEvent.affectedRows[0].should.be.an('object');

    triggerEvent.affectedRows[0].should.have.ownPropertyDescriptor('after');
    expect(triggerEvent.affectedRows[0].after).to.be.an('undefined');

    triggerEvent.affectedRows[0].should.have.ownPropertyDescriptor('before');
    triggerEvent.affectedRows[0].before.should.be.an('object');
    triggerEvent.affectedRows[0].before.should.have.ownPropertyDescriptor(TEST_COLUMN_1);
    triggerEvent.affectedRows[0].before[TEST_COLUMN_1].should.be.a('string').equals('test1');
    triggerEvent.affectedRows[0].before.should.have.ownPropertyDescriptor(TEST_COLUMN_2);
    triggerEvent.affectedRows[0].before[TEST_COLUMN_2].should.be.a('string').equals('test2');

    await instance.stop();
  }).timeout(10000);

  it('should catch events through an ALL trigger', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    }, {
      startAtEnd: true,
      excludedSchemas: {
        mysql: true,
      },
    });

    await instance.start();

    const triggerEvents = [];
    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA}.${TEST_TABLE}`,
      statement: MySQLEvents.STATEMENTS.ALL,
      callback: (event) => {
        triggerEvents.push(event);
      },
    });

    await delay(1000);

    await executeQuery(instance.connection, `USE ${TEST_SCHEMA};`);
    await executeQuery(instance.connection, `INSERT INTO ${TEST_TABLE} VALUES ('test1', 'test2');`);
    await executeQuery(instance.connection, `UPDATE ${TEST_TABLE} SET ${TEST_COLUMN_1} = 'test3', ${TEST_COLUMN_2} = 'test4';`);
    await executeQuery(instance.connection, `DELETE FROM ${TEST_TABLE} WHERE ${TEST_COLUMN_1} = 'test3' AND ${TEST_COLUMN_2} = 'test4';`);

    await delay(1000);

    expect(triggerEvents).to.be.an('array').that.is.not.empty;

    triggerEvents[0].should.have.ownPropertyDescriptor('type');
    triggerEvents[0].type.should.be.a('string').equals('INSERT');

    triggerEvents[1].should.have.ownPropertyDescriptor('type');
    triggerEvents[1].type.should.be.a('string').equals('UPDATE');

    triggerEvents[2].should.have.ownPropertyDescriptor('type');
    triggerEvents[2].type.should.be.a('string').equals('DELETE');

    await instance.stop();
  }).timeout(10000);

  it('should remove a previously added event trigger', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    }, {
      startAtEnd: true,
      excludedSchemas: {
        mysql: true,
      },
    });

    await instance.start();

    instance.addTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA}.${TEST_TABLE}`,
      statement: MySQLEvents.STATEMENTS.ALL,
      callback: () => {},
    });

    instance.triggers[`${TEST_SCHEMA}.${TEST_TABLE}`].statements[MySQLEvents.STATEMENTS.ALL].should.be.an('array').that.is.not.empty;

    instance.triggers[`${TEST_SCHEMA}.${TEST_TABLE}`].statements[MySQLEvents.STATEMENTS.ALL][0].should.be.an('object');
    instance.triggers[`${TEST_SCHEMA}.${TEST_TABLE}`].statements[MySQLEvents.STATEMENTS.ALL][0].name.should.be.a('string').equals('Test');
    instance.triggers[`${TEST_SCHEMA}.${TEST_TABLE}`].statements[MySQLEvents.STATEMENTS.ALL][0].callback.should.be.a('function');

    instance.removeTrigger({
      name: 'Test',
      expression: `${TEST_SCHEMA}.${TEST_TABLE}`,
      statement: MySQLEvents.STATEMENTS.ALL,
    });

    expect(instance.triggers[`${TEST_SCHEMA}.${TEST_TABLE}`].statements[MySQLEvents.STATEMENTS.ALL][0]).to.be.an('undefined');

    await instance.stop();
  }).timeout(10000);
});
