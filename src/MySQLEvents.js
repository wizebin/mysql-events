const debug = require('debuggler')();
const ZongJi = require('zongji');
const mysql = require('mysql');
const Connection = require('mysql/lib/Connection');
const EventEmitter = require('events');

const EVENTS = {
  CONNECTION_ERROR: 'connectionError',
  ZONGJI_ERROR: 'zongjiError',
};

class MySQLEvents extends EventEmitter {
  /**
   * @param {Object|Connection|String} connection
   * @param {Object} options
   */
  constructor(connection, options = {}) {
    super();

    this.connection = connection;
    this.options = options;

    this.zongJi = null;
    this.connected = false;
    this.triggers = [];
  }

  /**
   * @return {{CONNECTION_ERROR: string, ZONGJI_ERROR: string}}
   * @constructor
   */
  static get EVENTS() {
    return EVENTS;
  }

  _handleZongJiEvents() {
    this.zongJi.on('error', err => this.emit(EVENTS.ZONGJI_ERROR, err));

    this.zongJi.on('binlog', (event) => {
      console.log(event); // SHIT! https://github.com/nevill/zongji/pull/21
    });
  }

  _handleConnectionEvents() {
    this.connection.on('error', err => this.emit(EVENTS.CONNECTION_ERROR, err));
  }

  /**
   * @return {Promise<void>}
   */
  async start() {
    debug('connecting to mysql');

    if (typeof this.connection === 'string') {
      debug('creating connection from string:', this.connection);
      this.connection = mysql.createConnection(this.connection);
    }

    if ((typeof this.connection === 'object') && !(this.connection instanceof Connection)) {
      debug('creating connection from object:', this.connection);
      this.connection = mysql.createConnection(this.connection);
    }

    if ((this.connection instanceof Connection) && (this.connection.state !== 'connected')) {
      debug('initializing connection');
      await new Promise((resolve, reject) => {
        this.connection.connect((err) => {
          if (err) return reject(err);
          resolve();
        });
      });
    }

    debug('initializing zongji');
    this.zongJi = new ZongJi(this.connection, this.options);

    debug('connected');
    this.emit('connected');
    this._handleConnectionEvents();
    this._handleZongJiEvents();
    this.zongJi.start();
    this.connected = true;
  }

  /**
   * @return {Promise<void>}
   */
  async stop() {
    debug('disconnecting from mysql');

    await new Promise((resolve, reject) => {
      this.connection.end((err) => {
        if (err) return reject(err);
        resolve();
      });
    });

    this.zongJi.stop();
    this.zongJi = null;

    debug('disconnected');
    this.emit('disconnected');
    this.connected = false;
  }

  on(expression, statements) {
  }

  off() {

  }
}

module.exports = MySQLEvents;
