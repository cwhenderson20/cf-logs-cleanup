const cluster = require('cluster');
const async = require('async');

cluster.setupMaster({
	exec: 'lib/process-hour.js',
	silent: false
});

function processDay(config, listObjectResults, cb) {
	const { maxWorkers, prefix } = config;
	const workers = new Map();
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
	const completedHours = {};
	let nextHour = parseInt(dateParts[3], 10);

	console.log('Processing day', date.substring(0, date.length - 3));

	for (let i = nextHour; i < 24; i++) {
		completedHours[i] = false;
	}

	async.whilst(
		() => workers.size < maxWorkers,
		(whilstCb) => {
			const worker = cluster.fork({
				AWS_ACCESS_KEY_ID: config.aws.accessKeyId,
				AWS_SECRET_ACCESS_KEY: config.aws.secretAccessKey,
				BUCKET: config.bucket
			}).on('online', () => {
				workers.set(workers.size, worker);
				whilstCb();
			});
		},
		(err) => {
			if (err) {
				return cb(err);
			}

			cluster.on('message', (worker, message) => {
				completedHours[message.hour] = true;

				if (Object.keys(completedHours).every((hour) => completedHours[hour])) {
					console.log(`Finished processing ${date.substring(0, date.length - 3)}`);
					cluster.removeAllListeners('message');

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
				worker.on('exit', (code) => {
					if (code > 0) {
						return cb(new Error('Worker exited'));
					}
				});

				if (nextHour < 24) {
					console.log('Processing hour', nextHour);
					worker.send({ prefix, cfid, dateParts, nextHour });
					nextHour++;
				}
				eachCb();
			});
		}
	);
}

module.exports = processDay;
