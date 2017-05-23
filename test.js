const { init, start, stop } = require('./index.js');

init().then(() => {
	start().catch(err => console.log(err));
})

process.on('unhandledRejection', (reason) => {
	console.log(reason);
})