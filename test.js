/* eslint-disable padded-blocks */

const chai = require('chai');
const mysql = require('mysql');
const MySQLEvents = require('./lib');

before(() => {
  chai.should();
});

const delay = (timeout = 500) => new Promise((resolve) => {
  setTimeout(resolve, timeout);
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
  }).timeout(2000);

  it('should connect and disconnect from MySQL using a dsn', async () => {
    const instance = new MySQLEvents({
      host: 'localhost',
      user: 'root',
      password: 'root',
    });

    await instance.start();

    await delay();

    await instance.stop();
  }).timeout(2000);

});
