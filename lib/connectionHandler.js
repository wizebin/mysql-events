const debug = require('debuggler')();
const mysql = require('mysql');
const Connection = require('mysql/lib/Connection');

const connect = connection => new Promise((resolve, reject) => connection.connect((err) => {
  if (err) return reject(err);
  resolve();
}));

const connectionHandler = async (connection) => {
  if (typeof connection === 'string') {
    debug('creating connection from string:', connection);
    this.connection = mysql.createConnection(connection);
  }

  if ((typeof connection === 'object') && !(connection instanceof Connection)) {
    debug('creating connection from object:', connection);
    connection = mysql.createConnection(connection);
  }

  if (connection.state !== 'connected') {
    debug('initializing connection');
    await connect(connection);
  }

  return connection;
};

module.exports = connectionHandler;
