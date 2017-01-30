const os = require('os');
const AWS = require('aws-sdk');
const argv = require('yargs')
	.env()
	.option('bucket', {
		alias: 'b',
		describe: 'S3 bucket holding Cloudfront log files',
		demand: true,
		type: 'string'
	})
	.option('prefix', {
		alias: 'p',
		describe: 'Search prefix to append to all S3 requests',
		demand: false,
		type: 'string'
	})
	.option('partial-matching', {
		alias: 'm',
		describe: 'Enable partial prefix matching',
		demand: false,
		default: false,
		type: 'boolean'
	})
	.option('aws-access-key-id', {
		alias: 'k',
		describe: 'AWS access key id',
		demand: true,
		type: 'string'
	})
	.option('aws-secret-access-key', {
		alias: 's',
		describe: 'AWS secret access key',
		demand: true,
		type: 'string'
	})
	.option('max-workers', {
		alias: 'w',
		describe: 'Max number of node workers, defaults to num cpus',
		demand: false,
		type: 'number',
		default: os.cpus().length
	})
	.option('cfids', {
		alias: 'c',
		describe: 'Cloudfront distribution ids to search for',
		demand: false,
		type: 'array'
	})
	.option('days-to-keep', {
		alias: 'd',
		describe: 'Number of days of log history to keep unprocessed',
		demand: false,
		type: 'number',
		default: 45
	})
	.wrap(null)
	.argv;

const s3 = new AWS.S3({
	params: {
		Bucket: argv.bucket,
		Delimiter: '/'
	},
	accessKeyId: argv.awsAccessKeyId,
	secretAccessKey: argv.awsSecretAccessKey
});

const config = {
	s3,
	bucket: argv.bucket,
	prefix: parsePrefix(argv.prefix),
	partialMatching: argv.partialMatching,
	cfids: argv.cfids,
	maxWorkers: argv.maxWorkers,
	daysToKeep: argv.daysToKeep,
	aws: {
		accessKeyId: argv.awsAccessKeyId,
		secretAccessKey: argv.awsSecretAccessKey
	}
};

function parsePrefix(prefix) {
	if (!prefix) {
		return;
	}

	if (!argv.partialMatching && prefix.slice(-1) !== '/') {
		return `${prefix}/`;
	}

	return prefix;
}

module.exports = config;
