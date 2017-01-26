const { argv } = require("yargs");
const moment = require("moment");
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
		value: parsePrefix(argv.prefix || process.env.PREFIX),
		required: false
	},
	startDate: {
		argvName: "start",
		envVar: "START_DATE",
		value: parseDateInput(argv.start || process.env.START_DATE || "2013-01-01"),
		required: false
	},
	endDate: {
		argvName: "end",
		envVar: "END_DATE",
		value: parseDateInput(argv.end || process.env.END_DATE),
		required: false
	},
	awsAccessKey: {
		argvName: "aws-key",
		envVar: "AWS_ACCESS_KEY",
		value: argv["aws-key"] || process.env.AWS_ACCESS_KEY,
		required: true
	},
	awsSecret: {
		argvName: "aws-secret",
		envVar: "AWS_SECRET",
		value: argv["aws-secret"] || process.env.AWS_SECRET,
		required: true
	},
	live: {
		argvName: "live",
		envVar: "LIVE",
		value: argv.live || process.env.LIVE || false,
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

if (moment(options.startDate.value).isAfter(moment(options.endDate.value))) {
	throw new Error("End date must be after start date");
}

const config = {
	bucket: options.bucket.value,
	prefix: options.prefix.value,
	start: options.startDate.value,
	end: options.endDate.value,
	live: options.live.value,
	aws: {
		accessKeyId: options.awsAccessKey.value,
		secretAccessKey: options.awsSecret.value
	}
};

function parseDateInput(input) {
	if (!input) {
		return moment().utc();
	}

	if (Date.parse(input)) {
		return moment(input).utc();
	}

	const relativeParts = input.split(" ");
	if (relativeParts.length !== 2) {
		return moment().utc();
	}

	const relativeDate = moment().subtract(parseInt(relativeParts[0], 10), relativeParts[1]);
	if (!relativeDate.isValid()) {
		return moment().utc();
	}

	return relativeDate.utc();
}

function parsePrefix(prefix) {
	if (prefix.charAt(prefix.length - 1) !== "/") {
		return `${prefix}/`;
	}

	return prefix;
}

module.exports = config;
