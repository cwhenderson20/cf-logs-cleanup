#!/usr/bin/env node

const config = require("../lib/config");
const processLogs = require("../index");

processLogs(config, (err) => {
	if (err) {
		console.error(err);
		process.exit(1);
	}

	console.log("Completed without error");
	process.exit(0);
});
