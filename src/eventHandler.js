const utils = require('./utils');
const STATEMENTS = require('./STATEMENTS.enum');

const getEventType = (eventName) => {
  return {
    writerows: STATEMENTS.INSERT,
    updaterows: STATEMENTS.UPDATE,
    deleterows: STATEMENTS.DELETE,
  }[eventName];
};

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

const hasDifference = (beforeValue, afterValue) => {
  if ((beforeValue && afterValue) && beforeValue instanceof Date) {
    return beforeValue.getTime() !== afterValue.getTime();
  }

  return beforeValue !== afterValue;
};

const normalizeEvent = (event) => {
  if (!event.rows) return event;

  const eventType = getEventType(event.getEventName());

  const normalized = {
    eventType,
    affectedRows: [],
    affectedColumns: [],
    tableId: event.tableId,
    tableMap: event.tableMap,
  };

  event.rows.forEach((row) => {
    if (eventType === STATEMENTS.INSERT) {
      row = {
        before: undefined,
        after: row,
      };
    }
    if (eventType === STATEMENTS.DELETE) {
      row = {
        before: row,
        after: undefined,
      };
    }

    const normalizedRows = {
      after: normalizeRow(row.after),
      before: normalizeRow(row.before),
    };

    normalized.affectedRows.push(normalizedRows);

    const columns = Object.getOwnPropertyNames((normalizedRows.after || normalizedRows.before));
    for (let i = 0, len = columns.length; i < len; i += 1) {
      const columnName = columns[i];
      const beforeValue = (normalizedRows.before || {})[columnName];
      const afterValue = (normalizedRows.after || {})[columnName];

      if (hasDifference(beforeValue, afterValue)) {
        if (normalized.affectedColumns.indexOf(columnName) === -1) {
          normalized.affectedColumns.push(columnName);
        }
      }
    }
  });

  return normalized;
};

/**
 * @param {Object} event
 * @return {{findTriggers: findTriggers, validateTrigger: validateTrigger, executeTrigger: executeTrigger}}
 */
const eventHandler = (event) => {
  event = normalizeEvent(event);

  return {
    /**
     * @param {Object} triggers
     * @return {Object[]}
     */
    findTriggers: (triggers) => {
      const triggerExpressions = Object.getOwnPropertyNames(triggers);
      const { eventType } = event;

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
          const { eventType } = event;
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
