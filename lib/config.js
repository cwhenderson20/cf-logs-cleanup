const os = require("os");
const { argv } = require("yargs");
const options = {
	bucket: {
		argvName: "bucket",
		envVar: "BUCKET",
		value: argv.bucket || process.env.BUCKET,
		required: true
	},
	prefix: {
		argvName: "prefix",
		envVar: "PREFIX",
		value: argv.prefix || process.env.PREFIX,
		required: false
	},
	awsAccessKey: {
		argvName: "aws-key",
		envVar: "AWS_ACCESS_KEY",
		value: argv["aws-key"] || process.env.AWS_ACCESS_KEY_ID,
		required: false
	},
	awsSecret: {
		argvName: "aws-secret",
		envVar: "AWS_SECRET",
		value: argv["aws-secret"] || process.env.AWS_SECRET_ACCESS_KEY,
		required: false
	},
	maxWorkers: {
		argvName: "max-workers",
		envVar: "MAX_WORKERS",
		value: argv["max-workers"] || process.env.MAX_WORKERS || os.cpus().length,
		required: false
	}
};

const missingOptions = [];

Object.keys(options).forEach((option) => {
	if (!options[option].value && options[option].required) {
		missingOptions.push(options[option]);
	}
});

if (missingOptions.length) {
	const errorMessageParts = ["Missing required options. Please set the following:"];
	missingOptions.forEach((missingOption) => errorMessageParts.push(`  Flag --${missingOption.argvName} or environment variable ${missingOption.envVar}`));
	const errorMessage = errorMessageParts.join("\n");
	throw new Error(errorMessage);
}

const config = {
	bucket: options.bucket.value,
	prefix: options.prefix.value,
	maxWorkers: options.maxWorkers.value,
	aws: {
		accessKeyId: options.awsAccessKey.value,
		secretAccessKey: options.awsSecret.value
	}
};

module.exports = config;
