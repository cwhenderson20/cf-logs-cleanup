const fs = require('fs');
const zlib = require('zlib');
const readline = require('readline');
const async = require('async');
const concat = require('concat-files');
const debugLogger = require('debug')('cf');
const tmp = require('tmp');

function processHour(settings, cb) {
	const debug = new HourDebugger(settings.dateParts, settings.hour);
	settings.debug = debug;

	debug('Start processing');

	async.auto({
		getHourPartsList: wrap(getHourPartsList),
		downloadHourParts: ['getHourPartsList', wrap(downloadHourParts)],
		joinParts: ['downloadHourParts', wrap(joinParts)],
		unzip: ['joinParts', wrap(unzip)],
		clean: ['unzip', wrap(clean)],
		rezip: ['clean', wrap(rezip)],
		upload: ['rezip', wrap(upload)],
		cleanS3: ['upload', wrap(cleanS3)]
	}, (err, results) => {
		if (err) {
			console.log(err);
			return cb(err);
		}

		results.downloadHourParts && results.downloadHourParts.cleanupCb();
		cb();
	});

	function wrap(fn) {
		return (results, cb) => {
			if (cb) {
				fn(settings, results, cb);
			} else {
				cb = results;
				fn(settings, cb);
			}
		};
	}
}

function getHourPartsList(settings, cb) {
	const { config: { s3 }, debug } = settings;
	const Prefix = constructS3DownloadPrefix(settings);

	debug('Getting hour parts list');

	s3.listObjectsV2({ Prefix }, (err, data) => {
		if (err) {
			return cb(err);
		}

		const keys = data.Contents.map((object) => object.Key);
		cb(null, keys);
	});
}

function downloadHourParts({ config: { s3 }, debug }, results, cb) {
	const keys = results.getHourPartsList;

	if (keys.length === 0) {
		return cb();
	}

	debug(`Downloading ${keys.length} hour parts`);

	tmp.dir({ unsafeCleanup: true }, (err, path, cleanupCb) => {
		if (err) {
			return cb(err);
		}

		async.eachLimit(keys, 6, (key, eachCb) => {
			const filename = key.split('/').pop();

			if (!filename) {
				return eachCb();
			}

			const file = fs.createWriteStream(`${path}/${filename}`);
			const stream = s3.getObject({ Key: key }).createReadStream();

			stream.on('error', eachCb);
			stream.on('end', eachCb);

			stream.pipe(file);
		}, (err) => {
			if (err) {
				return cb(err);
			}

			cb(null, { cleanupCb, tmpDir: path });
		});
	});
}

function joinParts({ debug }, results, cb) {
	const keys = results.getHourPartsList;

	if (keys.length === 0) {
		return cb();
	}

	debug(`Joining ${keys.length} hour parts`);

	const { tmpDir } = results.downloadHourParts;
	const filesArray = keys.map((key) => `${tmpDir}/${key.split('/').pop()}`);
	const date = keys[0].split('.')[1];
	const outPath = `${tmpDir}/${date}.gz`;

	concat(filesArray, outPath, (err) => {
		if (err) {
			return cb(err);
		}

		cb(null, outPath);
	});
}

function unzip({ debug }, results, cb) {
	const keys = results.getHourPartsList;

	if (keys.length === 0) {
		return cb();
	}

	debug(`Unzipping ${keys.length} hour parts`);

	const combinedZipPath = results.joinParts;
	const unzipPath = `${combinedZipPath.substring(0, combinedZipPath.length - 3)}`;
	const input = fs.createReadStream(combinedZipPath);
	const output = fs.createWriteStream(unzipPath);
	const gunzip = zlib.createGunzip();

	output.on('error', cb);
	output.on('finish', () => cb(null, unzipPath));

	input.pipe(gunzip).pipe(output);
}

function clean({ debug }, results, cb) {
	const keys = results.getHourPartsList;

	if (keys.length === 0) {
		return cb();
	}

	debug('Cleaning joined file');

	const header = '#Version: 1.0\n#Fields: date time x-edge-location sc-bytes c-ip cs-method cs(Host) cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query cs(Cookie) x-edge-result-type x-edge-request-id x-host-header cs-protocol cs-bytes time-taken x-forwarded-for ssl-protocol ssl-cipher x-edge-response-result-type cs-protocol-version';
	const inputFile = fs.createReadStream(results.unzip);
	const outputPath = `${results.unzip}.txt`;
	const outputFile = fs.createWriteStream(outputPath);
	const rl = readline.createInterface({ input: inputFile });

	outputFile.write(header);

	rl.on('line', (line) => {
		if (line.charAt(0) !== '#') {
			outputFile.write(`${line}\n`);
		}
	});

	inputFile.on('end', (err) => {
		if (err) {
			return cb(err);
		}

		cb(null, outputFile.path);
	});
}

function rezip({ debug }, results, cb) {
	const keys = results.getHourPartsList;

	if (keys.length === 0) {
		return cb();
	}

	debug('Rezipping');

	const combinedFilePath = results.clean;
	const zipPath = `${combinedFilePath.substring(0, combinedFilePath.length - 4)}.gz`;
	const input = fs.createReadStream(combinedFilePath);
	const output = fs.createWriteStream(zipPath);
	const gzip = zlib.createGzip();

	output.on('error', cb);
	output.on('finish', () => cb(null, zipPath));

	input.pipe(gzip).pipe(output);
}

function upload(settings, results, cb) {
	const { config: { s3 }, debug } = settings;
	const keys = results.getHourPartsList;

	if (keys.length === 0) {
		return cb();
	}

	debug('Uploading to S3');

	const zipPath = results.rezip;
	const s3UploadPath = constructS3UploadPath(settings);
	const zipReadStream = fs.createReadStream(zipPath);

	s3.putObject({
		Key: s3UploadPath,
		Body: zipReadStream
	}, cb);
}

function cleanS3({ config: { s3 }, debug }, results, cb) {
	const keys = results.getHourPartsList;

	if (keys.length === 0) {
		return cb();
	}

	debug('Cleaning up S3');

	s3.deleteObjects({
		Delete: { Objects: results.getHourPartsList.map((item) => ({ Key: item })) }
	}, cb);
}

function constructS3DownloadPrefix({ prefix, cfid, dateParts, hour }) {
	const folder = prefix ? `${prefix.split('/').shift()}/` : '';
	return `${folder}${cfid}.${dateParts[0]}-${dateParts[1]}-${dateParts[2]}-${padHour(hour)}.`;
}

function constructS3UploadPath({ prefix, cfid, dateParts, hour }) {
	const folder = prefix ? `${prefix.split('/').shift()}/` : '';
	return `${folder}processed/${cfid}/${dateParts[0]}/${dateParts[1]}/${dateParts[2]}/${padHour(hour)}.gz`;
}

function padHour(hour) {
	if (hour < 10) {
		return `0${hour}`;
	}
	return hour;
}

function HourDebugger(dateParts, hour) {
	return (message) => debugLogger(`${dateParts[0]}-${dateParts[1]}-${dateParts[2]}-${hour}: ${message}`);
}

module.exports = processHour;
