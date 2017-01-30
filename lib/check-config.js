const os = require('os');
const AWS = require('aws-sdk');
const moment = require('moment');

function checkConfig(config) {
	if (!config || typeof config !== 'object') {
		throw new Error('Missing or invalid config object');
	}

	if (!config.bucket || typeof config.bucket !== 'string') {
		throw new Error('Missing or invalid required parameter \'bucket\'');
	}

	if (Object.hasOwnProperty.call(config, 'partialMatching') && typeof config.partialMatching !== 'boolean') {
		throw new Error('Parameter \'partialMatching\' must be type boolean');
	}

	if (config.cfids) {
		const error = new Error('Parameter \'cfids\' must either be a space-separated string or array of strings');

		if (typeof config.cfids !== 'string') {
			throw error;
		} else if (Array.isArray(config.cfids)) {
			if (!config.cfids.every((cfid) => typeof cfid === 'string')) {
				throw error;
			}
		} else {
			config.cfids = config.cfids.split(' ');
		}
	}

	if (Object.hasOwnProperty.call(config, 'maxWorkers') && typeof config.maxWorkers !== 'number') {
		throw new Error('Parameter \'maxWorkers\' must be type number');
	}

	if (Object.hasOwnProperty.call(config, 'daysToKeep') && typeof config.daysToKeep !== 'number') {
		throw new Error('Parameter \'daysToKeep\' must be type number');
	}

	if (Object.hasOwnProperty.call(config, 'aws')) {
		if (typeof config.aws !== 'object') {
			throw new Error('Parameter \'aws\' must be an object');
		}

		if (!config.aws.accessKeyId || !config.aws.secretAccessKey) {
			throw new Error('Parameter \'aws\' must have properties \'accessKeyId\' and \'secretAccessKey\'');
		}
	} else {
		throw new Error('aws config required');
	}

	config.s3 = new AWS.S3({
		params: {
			Bucket: config.bucket,
			Delimiter: '/'
		},
		accessKeyId: config.aws.accessKeyId,
		secretAccessKey: config.aws.secretAccessKey
	});

	const formattedConfig = {
		s3: config.s3,
		aws: {
			accessKeyId: config.aws.accessKeyId,
			secretAccessKey: config.aws.secretAccessKey
		},
		bucket: config.bucket,
		prefix: parsePrefix(config),
		partialMatching: config.partialMatching,
		cfids: config.cfids,
		maxWorkers: config.maxWorkers || os.cpus().length,
		daysToKeep: config.daysToKeep || 45,
		lastProcessableDay: moment().subtract(config.daysToKeep || 45, 'days').startOf('day')
	};

	return formattedConfig;
}

function parsePrefix(config) {
	if (!config.prefix) {
		return;
	}

	if (!config.partialMatching && config.prefix.slice(-1) !== '/') {
		return `${config.prefix}/`;
	}

	return config.prefix;
}

module.exports = checkConfig;
