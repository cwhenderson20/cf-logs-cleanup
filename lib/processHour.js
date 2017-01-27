const cluster = require("cluster");
const fs = require("fs");
const zlib = require("zlib");
const readline = require("readline");
const async = require("async");
const concat = require("concat-files");
const AWS = require("aws-sdk");
const tmp = require("tmp");
const config = require("./config");
const s3 = new AWS.S3({
	params: {
		Bucket: config.bucket,
		Delimiter: "/",
	},
	accessKeyId: config.aws.accessKeyId,
	secretAccessKey: config.aws.secretAccessKey
});

process.on("message", ({ cfid, dateParts, nextHour }) => {
	async.auto({
		getHourPartsList: (cb) => getHourPartsList(cfid, dateParts, nextHour, cb),
		downloadHourParts: ["getHourPartsList", (results, cb) => downloadHourParts(results, cb)],
		joinParts: ["downloadHourParts", joinParts],
		unzip: ["joinParts", unzip],
		clean: ["unzip", clean],
		rezip: ["clean", rezip],
		upload: ["rezip", (results, cb) => upload(cfid, dateParts, nextHour, results, cb)],
		cleanS3: ["upload", cleanS3]
	}, (err, results) => {
		results.downloadHourParts && results.downloadHourParts.cleanupCb();

		// console.log(`Finished processing ${dateParts[0]}-${dateParts[1]}-${dateParts[2]}-${padHour(nextHour)}`);

		if (err) {
			cluster.worker.kill();
		}

		process.send({ hour: nextHour });
	});
});

function getHourPartsList(cfid, dateParts, nextHour, cb) {
	const prefix = constructS3DownloadPrefix(cfid, dateParts, nextHour);

	s3.listObjectsV2({ Prefix: prefix }, (err, data) => {
		if (err) {
			return cb(err);
		}

		const keys = data.Contents.map((object) => object.Key);
		cb(null, keys);
	});
}

function downloadHourParts(results, cb) {
	const keys = results.getHourPartsList;

	if (!keys.length) {
		return cb();
	}

	tmp.dir({ unsafeCleanup: true }, (err, path, cleanupCb) => {
		if (err) {
			return cb(err);
		}

		// console.log("Getting parts");
		async.eachLimit(keys, 3, (key, eachCb) => {
			const filename = key.split("/").pop();

			if (!filename) {
				return eachCb();
			}

			const file = fs.createWriteStream(`${path}/${filename}`);
			const stream = s3.getObject({ Key: key }).createReadStream();

			stream.on("error", eachCb);
			stream.on("end", eachCb);

			stream.pipe(file);
		}, (err) => {
			if (err) {
				return cb(err);
			}

			cb(null, { cleanupCb, tmpDir: path });
		});
	});
}

function joinParts(results, cb) {
	const keys = results.getHourPartsList;

	if (!keys.length) {
		return cb();
	}

	// console.log("Joining parts");

	const { tmpDir } = results.downloadHourParts;
	const filesArray = keys.map((key) => `${tmpDir}/${key.split("/").pop()}`);
	const date = keys[0].split(".")[1];
	const outPath = `${tmpDir}/${date}.gz`;

	concat(filesArray, outPath, (err) => {
		if (err) {
			return cb(err);
		}

		cb(null, outPath);
	});
}

function unzip(results, cb) {
	const keys = results.getHourPartsList;

	if (!keys.length) {
		return cb();
	}

	// console.log("Unzipping");

	const combinedZipPath = results.joinParts;
	const unzipPath = `${combinedZipPath.substring(0, combinedZipPath.length - 3)}`;
	const input = fs.createReadStream(combinedZipPath);
	const output = fs.createWriteStream(unzipPath);
	const gunzip = zlib.createGunzip();

	output.on("error", cb);
	output.on("finish", () => cb(null, unzipPath));

	input.pipe(gunzip).pipe(output);
}

function clean(results, cb) {
	const keys = results.getHourPartsList;

	if (!keys.length) {
		return cb();
	}

	// console.log("Cleaning joined file");

	const header = "#Version: 1.0\n#Fields: date time x-edge-location sc-bytes c-ip cs-method cs(Host) cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query cs(Cookie) x-edge-result-type x-edge-request-id x-host-header cs-protocol cs-bytes time-taken x-forwarded-for ssl-protocol ssl-cipher x-edge-response-result-type cs-protocol-version";
	const inputFile = fs.createReadStream(results.unzip);
	const outputPath = `${results.unzip}.txt`;
	const outputFile = fs.createWriteStream(outputPath);
	const rl = readline.createInterface({ input: inputFile });

	outputFile.write(header);

	rl.on("line", (line) => {
		if (line.charAt(0) !== "#") {
			outputFile.write(`${line}\n`);
		}
	});

	inputFile.on("end", (err) => {
		if (err) {
			return cb(err);
		}

		cb(null, outputFile.path);
	});
}

function rezip(results, cb) {
	const keys = results.getHourPartsList;

	if (!keys.length) {
		return cb();
	}

	// console.log("Rezipping");

	const combinedFilePath = results.clean;
	const zipPath = `${combinedFilePath.substring(0, combinedFilePath.length - 4)}.gz`;
	const input = fs.createReadStream(combinedFilePath);
	const output = fs.createWriteStream(zipPath);
	const gzip = zlib.createGzip();

	output.on("error", cb);
	output.on("finish", () => cb(null, zipPath));

	input.pipe(gzip).pipe(output);
}

function upload(cfid, dateParts, nextHour, results, cb) {
	const keys = results.getHourPartsList;

	if (!keys.length) {
		return cb();
	}

	// console.log("Uploading to S3");

	const zipPath = results.rezip;
	const s3UploadPath = constructS3UploadPath(cfid, dateParts, nextHour);
	const zipReadStream = fs.createReadStream(zipPath);

	s3.putObject({
		Key: s3UploadPath,
		Body: zipReadStream
	}, cb);
}

function cleanS3(results, cb) {
	const keys = results.getHourPartsList;

	if (!keys.length) {
		return cb();
	}

	// console.log("Cleaning up S3");

	s3.deleteObjects({
		Delete: { Objects: results.getHourPartsList.map((item) => ({ Key: item })) }
	}, cb);
}

function constructS3DownloadPrefix(cfid, dateParts, nextHour) {
	return `${config.prefix}${cfid}.${dateParts[0]}-${dateParts[1]}-${dateParts[2]}-${padHour(nextHour)}.`;
}

function constructS3UploadPath(cfid, dateParts, nextHour) {
	return `${config.prefix}processed/${cfid}/${dateParts[0]}/${dateParts[1]}/${dateParts[2]}/${padHour(nextHour)}.gz`;
}

function padHour(hour) {
	if (hour < 10) {
		return `0${hour}`;
	}
	return hour;
}
