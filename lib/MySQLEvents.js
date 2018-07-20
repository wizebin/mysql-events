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
    this.expressions = {};
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
    const triggers = eventHandler.findTriggers(event, this.expressions);

    Promise.all(triggers.map(async (trigger) => {
      try {
        await trigger.callback(event);
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
   * @param {Object} [options = {}]
   * @return {Promise<void>}
   */
  async start(options = {}) {
    if (this.started) return;
    debug('connecting to mysql');
    this.connection = await connectionHandler(this.connection);

    debug('initializing zongji');
    this.zongJi = new ZongJi(this.connection, Object.assign(this.options, options));

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

    this.zongJi.stop();
    this.zongJi = null;

    await new Promise((resolve, reject) => {
      this.connection.end((err) => {
        if (err) return reject(err);
        resolve();
      });
    });

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

    this.expressions[expression] = this.expressions[expression] || {};
    this.expressions[expression].statements = this.expressions[expression].statements || {};
    this.expressions[expression].statements[statement] = this.expressions[expression].statements[statement] || [];

    const triggers = this.expressions[expression].statements[statement];
    if (triggers.find(st => st.name === name)) {
      throw new Error(`There's already a trigger named "${name}" for expression "${expression}" with statement "${statement}"`);
    }

    triggers.push({
      name,
      callback,
    });
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
    const exp = this.expressions[expression];
    if (!exp) return;

    const triggers = exp.statements[statement];
    if (!triggers) return;

    const named = triggers.find(st => st.name === name);
    if (!named) return;

    const index = triggers.indexOf(named);
    triggers.splice(index, 1);
  }
}

module.exports = MySQLEvents;
