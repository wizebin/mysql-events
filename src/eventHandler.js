const STATEMENTS = require('./STATEMENTS.enum');

const getEventType = (eventName) => {
  return {
    writerows: STATEMENTS.INSERT,
    updaterows: STATEMENTS.UPDATE,
    deleterows: STATEMENTS.DELETE,
  }[eventName];
};

const parseExpression = (expression = '') => {
  const parts = expression.split('.');
  return {
    schema: parts[0],
    table: parts[1],
    column: parts[2],
    value: parts[3],
  };
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

  const type = getEventType(event.getEventName());
  const schema = event.tableMap[event.tableId].parentSchema;
  const table = event.tableMap[event.tableId].tableName;

  const normalized = {
    type,
    schema,
    table,
    affectedRows: [],
    affectedColumns: [],
  };

  event.rows.forEach((row) => {
    if (type === STATEMENTS.INSERT) {
      row = {
        before: undefined,
        after: row,
      };
    }
    if (type === STATEMENTS.DELETE) {
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
      if (!event.type) return [];

      const triggerExpressions = Object.getOwnPropertyNames(triggers);
      const statements = [];

      for (let i = 0, len = triggerExpressions.length; i < len; i += 1) {
        const expression = triggerExpressions[i];
        const trigger = triggers[expression];

        const parts = parseExpression(expression);
        if (parts.schema !== '*' && parts.schema !== event.schema) return false;
        if (!(!parts.table || parts.table === '*') && parts.table !== event.table) return false;
        if (!(!parts.column || parts.column === '*') && event.affectedColumns.indexOf(parts.column) === -1) return false;

        if (trigger.statements[STATEMENTS.ALL]) statements.push(...trigger.statements[STATEMENTS.ALL]);
        if (trigger.statements[event.type]) statements.push(...trigger.statements[event.type]);
      }

      return statements;
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
