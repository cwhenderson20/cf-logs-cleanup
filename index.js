const config = require("./lib/config");

const async = require("async");
const AWS = require("aws-sdk");
const bunyan = require("bunyan");
const moment = require("moment");
const processHour = require("./lib/processHour");
const processDay = require("./lib/processDay");

const lastProcessableDate = moment().subtract(45, "days").startOf("day");
const s3 = new AWS.S3({
	params: {
		Bucket: config.bucket,
		Delimiter: "/",
	},
	accessKeyId: config.aws.accessKeyId,
	secretAccessKey: config.aws.secretAccessKey
});
let listObjectResults;

async.during(
	(cb) => {
		s3.listObjectsV2({ MaxKeys: 2, Prefix: config.prefix }, (err, data) => {
			if (err) {
				return cb(err);
			}

			if (!data.Contents.length) {
				return cb(null, false);
			}

			listObjectResults = data;

			if (data.Contents[0].Key.slice(-1) === "/") {
				if (data.Contents[1]) {
					return cb(null, shouldProcess(data.Contents[1].Key));
				}
				return cb();
			}

			cb(null, shouldProcess(data.Contents[0].Key));
		});
	},
	(cb) => processDay(s3, listObjectResults, cb),
	(err) => {
		if (err) {
			console.error(err);
			process.exit(1);
		}

		console.log("DONE");
		process.exit(0);
	}
);

function shouldProcess(key) {
	const filename = key.substring(key.lastIndexOf(config.prefix) + config.prefix.length);
	const dateTime = filename.split(".")[1];
	const date = moment(dateTime.substring(0, 10));

	return date.isBefore(lastProcessableDate);
}
