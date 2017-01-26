const fs = require("fs");
const zlib = require("zlib");
const readline = require("readline");
const async = require("async");
const concat = require("concat-files");
const tmp = require("tmp");
const config = require("./config");
const { isInDateRange } = require("./util");

function processHour(s3, cfid, date, cb) {
	async.auto({
		getHourPartsList: (cb) => getHourPartsList(s3, cfid, date, cb),
		downloadHourParts: ["getHourPartsList", (results, cb) => downloadHourParts(results, s3, cb)],
		joinParts: ["downloadHourParts", joinParts],
		unzip: ["joinParts", unzip],
		clean: ["unzip", clean],
		rezip: ["clean", rezip],
		upload: ["rezip", (results, cb) => upload(s3, cfid, date, results, cb)],
	}, (err, results) => {
		if (err) {
			return cb(err);
		}
		console.log(results);
		date.add(1, "hour");
		cb();
	});
}

function getHourPartsList(s3, cfid, date, cb) {
	if (!isInDateRange(date)) {
		return cb();
	}

	const prefix = constructS3DownloadPrefix(cfid, date);
	// const prefix = config.prefix;

	s3.listObjectsV2({ Prefix: prefix }, (err, data) => {
		if (err) {
			return cb(err);
		}

		const keys = data.Contents.map((object) => object.Key);
		cb(null, keys);
	});
}

function downloadHourParts(results, s3, cb) {
	const keys = results.getHourPartsList;

	if (!keys.length) {
		return cb();
	}

	tmp.dir({ unsafeCleanup: false }, (err, path, cleanupCb) => {
		if (err) {
			return cb(err);
		}

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
			console.log("Finished getting parts");
			if (err) {
				return cb(err);
			}

			cb(null, { cleanupCb, tmpDir: path });
		});
	});
}

function joinParts(results, cb) {
	console.log("Joining parts");
	const keys = results.getHourPartsList;

	if (!keys.length) {
		return cb();
	}

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

	const combinedFilePath = results.clean;
	const zipPath = `${combinedFilePath.substring(0, combinedFilePath.length - 4)}.gz`;
	const input = fs.createReadStream(combinedFilePath);
	const output = fs.createWriteStream(zipPath);
	const gzip = zlib.createGzip();

	output.on("error", cb);
	output.on("finish", () => cb(null, zipPath));

	input.pipe(gzip).pipe(output);
}

function upload(s3, cfid, date, results, cb) {
	const keys = results.getHourPartsList;

	if (!keys.length) {
		return cb();
	}

	const zipPath = results.rezip;
	const s3UploadPath = constructS3UploadPath(cfid, date);
	const zipReadStream = fs.createReadStream(zipPath);

	s3.putObject({
		Key: s3UploadPath,
		Body: zipReadStream
	}, cb);
}

function constructS3DownloadPrefix(cfid, date) {
	return `${config.prefix}${cfid}.${leftPad(date, "year")}-${leftPad(date, "month")}-${leftPad(date, "date")}-${leftPad(date, "hour")}.`;
}

function constructS3UploadPath(cfid, date) {
	return `${config.prefix}${cfid}/${leftPad(date, "year")}/${leftPad(date, "month")}/${leftPad(date, "date")}/${leftPad(date, "hour")}.gz`;
}

function leftPad(date, unit) {
	if (unit === "month") {
		const month = date.month() + 1;
		if (month < 10) {
			return `0${month}`;
		}
		return month;
	}

	if (date[unit]() < 10) {
		return `0${date[unit]()}`;
	}

	return date[unit]();
}

module.exports = processHour;
