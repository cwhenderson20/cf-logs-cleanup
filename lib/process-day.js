const async = require('async');
const processHour = require('./process-hour');

function processDay(config, listObjectResults, cb) {
	const { prefix, logger } = config;
	let firstObject = listObjectResults.Contents[0];

	if (firstObject.Key.slice(-1) === '/') {
		firstObject = listObjectResults.Contents[1];
	}

	if (!firstObject) {
		return cb();
	}

	const filename = firstObject.Key.split('/').pop();
	const filenameParts = filename.split('.');
	const cfid = filenameParts[0];
	const date = filenameParts[1];
	const dateParts = date.split('-');
	const hours = [];
	const firstHour = parseInt(dateParts[3], 10);

	logger.info('Processing day', date.substring(0, date.length - 3));

	for (let i = firstHour; i < 24; i++) {
		hours.push(i);
	}

	async.eachLimit(
		hours, 4,
		(hour, cb) => processHour({ config, cfid, prefix, dateParts, hour }, cb),
		cb
	);
}

module.exports = processDay;
