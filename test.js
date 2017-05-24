const MongoVersioning = require('./index.js');
const { MongoClient } = require('mongodb');

const mongoVersioning = new MongoVersioning();

mongoVersioning.init()
	.then(() => mongoVersioning.start())
	// .then(() => MongoClient.connect('mongodb://localhost:27018/test'))
	// .then(db => )

process.on('unhandledRejection', (reason) => {
	console.log(reason);
})