const debug = require('debuggler')();
const mysql = require('@vlasky/mysql');


const connect = connection => new Promise((resolve, reject) => connection.connect((err) => {
  if (err) return reject(err);
  resolve();
}));

const connectionHandler = async (connection) => {
  if (connection instanceof mysql.Pool) {
    debug('reusing pool:', connection);
    if (connection._closed) {
      connection = mysql.createPool(connection.config.connectionConfig);
    }
  }

  if (connection instanceof mysql.Connection) {
    debug('reusing connection:', connection);
    if (connection.state !== 'connected') {
      connection = mysql.createConnection(connection.config);
    }
  }

  if (typeof connection === 'string') {
    debug('creating connection from string:', connection);
    connection = mysql.createConnection(connection);
  }

  if ((typeof connection === 'object') && (!(connection instanceof mysql.Connection) && !(connection instanceof mysql.Pool))) {
    debug('creating connection from object:', connection);
    if (connection.isPool) {
      connection = mysql.createPool(connection);
    } else {
      connection = mysql.createConnection(connection);
    }
  }

  if ((connection instanceof mysql.Connection) && (connection.state !== 'connected')) {
    debug('initializing connection');
    console.log('test from here');
    await connect(connection);
  }

  return connection;
};

module.exports = connectionHandler;
