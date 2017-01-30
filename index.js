const async = require("async");
const debug = require("debug")("cf");
const moment = require("moment");
const checkConfig = require("./lib/checkConfig");
const processDay = require("./lib/processDay");

let listObjectResults;

function processLogs(config, cb) {
	config = checkConfig(config);

	if (config.cfids && config.cfids.length) {
		debug("CFID(s) specified, processing each in series");
		async.eachLimit(
			config.cfids, 1,
			(cfid, eachCb) => processDistribution(config, cfid, eachCb),
			logAndReturn
		);
	} else {
		debug("No CFIDs specified, processing all records");
		processDistribution(config, null, logAndReturn);
	}

	function logAndReturn(err) {
		if (err) {
			debug("Completed with errors");
			return cb(err);
		}

		debug("Completed without error");
		cb();
	}
}

function processDistribution(config, cfid, cb) {
	const prefix = `${config.prefix || ""}${cfid || ""}`;

	debug(`Searching for records in ${config.bucket}/${prefix}`);

	async.during(
		(cb) => probe(config, prefix, null, cb),
		(cb) => processDay(config, listObjectResults, cb),
		cb
	);
}

function probe(config, prefix, token, cb) {
	const { s3 } = config;
	const listObjectsConfig = { MaxKeys: 2, Prefix: prefix, ContinuationToken: token };

	s3.listObjectsV2(listObjectsConfig, (err, data) => {
		if (err) {
			return cb(err);
		}

		if (!data.Contents.length && !data.IsTruncated) {
			debug("No results returned and response is not truncated, exiting");
			return cb(null, false);
		}

		if (!data.Contents.length && data.IsTruncated) {
			debug("No results returned but response is truncated, probing again");
			return probe(config, prefix, data.NextContinuationToken, cb);
		}

		listObjectResults = data;

		if (data.Contents[0].Key.slice(-1) === "/") {
			debug("First result is a folder");

			if (data.Contents[1]) {
				return analyzeResult(1);
			}
		}

		debug("First result is a file");
		analyzeResult(0);

		function analyzeResult(index) {
			const isProcessable = shouldProcess(data.Contents[index].Key, config.lastProcessableDate);

			if (isProcessable) {
				debug(`Result [${index}] is processable`);
				return cb(null, true);
			}

			if (data.isTruncated) {
				debug(`Result [${index}] is not processable, but response is truncated, probing again`);
				return probe(config, prefix, data.NextContinuationToken, cb);
			}

			debug("Results are not processable");
			cb();
		}
	});
}

function shouldProcess(key, lastProcessableDate) {
	const filename = key.split("/").pop();
	const dateTime = filename.split(".")[1] || "";
	const dateTimeParts = dateTime.split("-").map((part) => {
		if (isNaN(parseInt(part, 10))) {
			return false;
		}
		return parseInt(part, 10);
	});

	debug(`Inspecting ${filename}`);

	if (filename.split(".").length !== 4 || dateTimeParts.length !== 4) {
		debug("Filename doesn't match pattern; not processable");
		return false;
	}

	if (!dateTimeParts.every((part) => typeof part === "number")) {
		debug("Date parts are not all numbers; not processable");
		return false;
	}

	const date = moment([dateTimeParts[0], dateTimeParts[1] - 1, dateTimeParts[2], dateTimeParts[3]]);
	const processable = date.isBefore(lastProcessableDate);

	if (!date.isValid()) {
		debug("Invalid date when constructed; not processable");
		return false;
	}

	if (processable) {
		debug("File falls within acceptable date range; proceeding");
	} else {
		debug("File is after processable date range");
	}

	return processable;
}

module.exports = processLogs;
