const config = require("./config");
const moment = require("moment");

function isInDateRange(date) {
	console.log(`Comparing ${date.toDate()} to ${config.start.toDate()} and ${config.end.toDate()}`);
	return moment(date).isBetween(config.start, config.end, "hour", "[]");
}

module.exports = { isInDateRange };
