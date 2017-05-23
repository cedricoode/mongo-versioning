const MongoVersioning = require('./index.js');

const mongoVersioning = new MongoVersioning();

mongoVersioning.init().then(() => {
	mongoVersioning.start();
})

process.on('unhandledRejection', (reason) => {
	console.log(reason);
})