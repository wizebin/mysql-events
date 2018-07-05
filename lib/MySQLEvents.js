const debug = require('debuggler')();
const ZongJi = require('@rodrigogs/zongji');
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
    this.started = false;
    this.triggers = {};
  }

  /**
   * @return {{BINLOG, TRIGGER_ERROR, CONNECTION_ERROR, ZONGJI_ERROR}}
   * @constructor
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

    Promise.all(triggers.map(async (trigger) => {
      try {
        trigger.callback(event);
      } catch (error) {
        this.emit(EVENTS.TRIGGER_ERROR, { trigger, error });
      }
    })).then(() => debug('triggers executed'));
  }

  /**
   * @private
   */
  _handleZongJiEvents() {
    this.zongJi.on('error', err => this.emit(EVENTS.ZONGJI_ERROR, err));
    this.zongJi.on('binlog', (event) => {
      this.emit(EVENTS.BINLOG, event);
      this._handleEvent(event);
    });
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
    this.started = false;
  }

  /**
   * @param {String} name
   * @param {String} expression
   * @param {String} [statement = 'ALL']
   * @param {Function} [callback]
   * @return {void}
   */
  addTrigger({
    name,
    expression,
    statement = STATEMENTS.ALL,
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
   * @param {String} name
   * @param {String} expression
   * @param {String} [statement = 'ALL']
   * @return {void}
   */
  removeTrigger({
    name,
    expression,
    statement = STATEMENTS.ALL,
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
