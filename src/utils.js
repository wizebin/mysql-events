const utils = {
  flatten: list => list.reduce(
    (a, b) => a.concat(Array.isArray(b) ? utils.flatten(b) : b), [],
  ),
};

module.exports = utils;
