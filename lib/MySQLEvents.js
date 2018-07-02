const debug = require('debuggler')();
const ZongJi = require('zongji');
const EventEmitter = require('events');
const eventHandler = require('./eventHandler');
const connectionHandler = require('./connectionHandler');

const EVENTS = require('./EVENTS.enum');
const STATEMENTS = require('./STATEMENTS.enum');

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
    this.started = false;
    this.triggers = {};
  }

  /**
   * @return {{CONNECTION_ERROR: string, ZONGJI_ERROR: string}}
   */
  static get EVENTS() {
    return EVENTS;
  }

  /**
   * @return {{ALL: string, INSERT: string, UPDATE: string, DELETE: string}}
   */
  static get STATEMENTS() {
    return STATEMENTS;
  }

  /**
   * @param {Object} event binlog event object.
   * @private
   */
  _handleEvent(event) {
    event = eventHandler.normalizeEvent(event);
    const triggers = eventHandler.findTriggers(event, this.triggers);

    Promise.all(triggers.map(trigger => eventHandler.executeTrigger(event, trigger)))
      .then(() => debug('triggers executed'));
  }

  /**
   * @private
   */
  _handleZongJiEvents() {
    this.zongJi.on('error', err => this.emit(EVENTS.ZONGJI_ERROR, err));
    this.zongJi.on('binlog', event => this._handleEvent(event));
  }

  /**
   * @private
   */
  _handleConnectionEvents() {
    this.connection.on('error', err => this.emit(EVENTS.CONNECTION_ERROR, err));
  }

  /**
   * @return {Promise<void>}
   */
  async start() {
    if (this.started) return;
    debug('connecting to mysql');
    this.connection = await connectionHandler(this.connection);

    debug('initializing zongji');
    this.zongJi = new ZongJi(this.connection, this.options);

    debug('connected');
    this.emit('connected');
    this._handleConnectionEvents();
    this._handleZongJiEvents();
    this.zongJi.start(this.options);
    this.connected = true;
    this.started = true;
  }

  /**
   * @return {Promise<void>}
   */
  async stop() {
    if (!this.started) return;
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
    this.started = false;
  }

  /**
   * @param {String} expression
   * @param {String} [statement = 'ALL']
   * @param {String} name
   * @param {Function} [callback]
   * @return {void}
   */
  addTrigger({
    expression,
    statement = STATEMENTS.ALL,
    name,
    callback,
  }) {
    if (!name) throw new Error('Missing trigger name');
    if (!expression) throw new Error('Missing trigger expression');

    const trigger = this.triggers[expression] || {};
    trigger.statements = trigger.statements || {};
    trigger.statements[statement] = trigger.statements[statement] || [];

    const statmnt = trigger.statements[statement];
    if (statmnt.find(st => st.name === name)) {
      throw new Error(`There's already a trigger named "${name}" for expression "${expression}" with statement "${statement}"`);
    }

    statmnt.push({
      name,
      callback,
    });

    this.triggers[expression] = trigger;
  }

  /**
   * @param {String} expression
   * @param {String} [statement = 'ALL']
   * @param {String} name
   * @return {void}
   */
  removeTrigger({
    expression,
    statement = STATEMENTS.ALL,
    name,
  }) {
    const trigger = this.triggers[expression];
    if (!trigger) return;

    const statmnt = trigger.statements[statement];
    if (!statmnt) return;

    const named = statmnt.find(st => st.name === name);
    if (!named) return;

    const index = statmnt.indexOf(named);
    statmnt.splice(index, 1);
  }
}

module.exports = MySQLEvents;
