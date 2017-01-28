const async = require("async");
const cluster = require("cluster");
const config = require("./config");

cluster.setupMaster({
	exec: "lib/processHour.js",
	silent: false
});

function processDay(s3, listObjectResults, cb) {
	const workers = new Map();
	let nextHour = 0;
	let firstObject = listObjectResults.Contents[0];

	if (firstObject.Key.slice(-1) === "/") {
		firstObject = listObjectResults.Contents[1];
	}

	if (!firstObject) {
		return cb();
	}

	const filename = firstObject.Key.split("/").pop();
	const filenameParts = filename.split(".");
	const cfid = filenameParts[0];
	const date = filenameParts[1];
	const dateParts = date.split("-");

	console.log("Processing day", date.substring(0, date.length - 3));

	nextHour = parseInt(dateParts[3], 10);

	const completedHours = {};

	for (let i = 0; i < 24; i++) {
		if (i < nextHour) {
			completedHours[i] = true;
		} else {
			completedHours[i] = false;
		}
	}

	async.whilst(
		() => workers.size < config.maxWorkers,
		(cb) => {
			const worker = cluster.fork().on("online", () => {
				workers.set(workers.size, worker);
				cb();
			});
		},
		(err) => {
			if (err) {
				return cb(err);
			}

			cluster.on("message", (worker, message) => {
				completedHours[message.hour] = true;

				if (Object.keys(completedHours).every((hour) => completedHours[hour])) {
					console.log(`Finished processing ${date.substring(0, date.length - 3)}`);
					cluster.removeAllListeners("message");

					for (const [, wk] of workers) {
						wk.kill();
					}
					return cb();
				}

				if (nextHour < 24) {
					console.log(`Processing hour ${nextHour}`);
					worker.send({ cfid, dateParts, nextHour });
					nextHour++;
				}
			});

			async.eachLimit(workers, 1, (workerRecord, eachCb) => {
				const worker = workerRecord[1];
				worker.on("exit", (code) => {
					if (code > 0) {
						return cb(new Error("Worker exited"));
					}
				});

				if (nextHour < 24) {
					console.log("Processing hour", nextHour);
					worker.send({ cfid, dateParts, nextHour });
					nextHour++;
				}
				eachCb();
			});
		}
	);
}

module.exports = processDay;
