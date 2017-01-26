const readline = require('readline');
const fs = require('fs');
const output = fs.createWriteStream('redacted-2.txt');

const rl = readline.createInterface({
	input: fs.createReadStream('combined.txt'),
});

rl.on('line', (line) => {
	if (line.indexOf("#") !== 0) {
		output.write(`${line}\n`);
	}
});
