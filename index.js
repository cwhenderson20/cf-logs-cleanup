const config = require("./lib/config");

const async = require("async");
const AWS = require("aws-sdk");
const moment = require("moment");
const bunyan = require("bunyan");
const processHour = require("./lib/processHour");
const { isInDateRange } = require("./lib/util");

const logger = bunyan.createLogger({ name: "cf-logs-cleanup" });
const s3 = new AWS.S3({
	params: {
		Bucket: config.bucket,
		Delimiter: "/",
	},
	accessKeyId: config.aws.accessKeyId,
	secretAccessKey: config.aws.secretAccessKey
});

console.log(config.prefix);

s3.listObjectsV2({ MaxKeys: 2, Prefix: config.prefix }, (err, data) => {
	if (err) {
		console.log(err);
		process.exit(1);
	}

	console.log(data);

	if (data.Contents.length === 2) {
		const firstObject = data.Contents[1];
		const filename = firstObject.Key.substring(firstObject.Key.lastIndexOf(config.prefix) + config.prefix.length);
		const cfid = filename.split(".").shift();
		const currentDate = moment(config.start);

		console.log(cfid);

		async.whilst(
			() => isInDateRange(currentDate),
			(cb) => processHour(s3, cfid, currentDate, cb),
			(err) => {
				if (err) {
					console.error(err);
					process.exit(1);
				}

				console.log("DONE");
				process.exit(0);
			}
		);
	}
});
