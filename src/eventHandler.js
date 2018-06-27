const utils = require('./utils');
const STATEMENTS = require('./STATEMENTS.enum');

const normalize = (event) => {
  const normalizeRow = (row) => {
    if (!row) return undefined;

    const columns = Object.getOwnPropertyNames(row);
    for (let i = 0, len = columns.length; i < len; i += 1) {
      const columnValue = row[columns[i]];

      if (columnValue instanceof Buffer && columnValue.length === 1) { // It's a boolean
        row[columns[i]] = (columnValue[0] > 0);
      }
    }

    return row;
  };

  if (event.rows) {
    event.rows = event.rows.map(row => ({
      after: normalizeRow(row.after),
      before: normalizeRow(row.before),
    }));
  }

  return event;
};

const getEventType = (eventName) => {
  return {
    writerows: STATEMENTS.INSERT,
    updaterows: STATEMENTS.UPDATE,
    deleterows: STATEMENTS.DELETE,
  }[eventName];
};

/**
 * @param {Object} event
 * @return {{findTriggers: findTriggers, validateTrigger: validateTrigger, executeTrigger: executeTrigger}}
 */
const eventHandler = (event) => {
  event = normalize(event);

  return {
    /**
     * @param {Object} triggers
     * @return {Object[]}
     */
    findTriggers: (triggers) => {
      const triggerExpressions = Object.getOwnPropertyNames(triggers);
      const eventType = getEventType(event.getEventName());

      if (!eventType) return [];

      const triggerStatements = triggerExpressions
        .map((te) => {
          const trigger = triggers[te];
          trigger.expression = te;
          return trigger;
        })
        .filter((trigger) => {
          const eventSchema = event.tableMap[event.tableId].parentSchema;
          const eventTable = event.tableMap[event.tableId].tableName;

          if (trigger.expression.indexOf(eventSchema) !== -1 || trigger.expression.indexOf(eventTable) !== -1) {
            if (trigger.statements[STATEMENTS.ALL] || trigger.statements[eventType]) {
              return true;
            }
          }

          return false;
        })
        .map((trigger) => {
          const eventType = getEventType(event.getEventName());
          const statements = [];
          if (trigger.statements[STATEMENTS.ALL]) {
            statements.push(trigger.statements[STATEMENTS.ALL]);
          }

          if (trigger.statements[eventType]) {
            statements.push(trigger.statements[eventType]);
          }

          return statements;
        });

      return utils.flatten(triggerStatements);
    },

    /**
     * @param {Object} trigger
     * @return {Boolean}
     */
    validateTrigger: (trigger) => {
      if (!trigger.validator) return true;

      return !!trigger.validator(event);
    },

    /**
     * @param {Object} trigger
     * @return {Promise<void>}
     */
    executeTrigger: async (trigger) => {
      try {
        await trigger.callback(event);
      } catch (err) {
        console.error(err);
      }
    },
  };
};

module.exports = eventHandler;
