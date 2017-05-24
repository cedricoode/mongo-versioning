const MongoVersioning = require('./index.js');
const { MongoClient } = require('mongodb');

const mongoVersioning = new MongoVersioning();

mongoVersioning.init()
	.then(() => mongoVersioning.start())
	.then(() => MongoClient.connect('mongodb://localhost:27018/test'))
	.then(db => {
		for (let i = 0; i < 10; i++) {
			db.collection('patients').insert({name: 'Yao', age: i});
		}
		for (let i = 0; i < 10; i++) {
			let i = Math.floor(Math.random() * 10) % 10;
			db.collection('patients').update({name: 'Yao', age: i}, {randomStr: generateRandomStr()})
		}
	})

process.on('unhandledRejection', (reason) => {
	console.log(reason);
});

function generateRandomStr() {
	 length = Math.random() * 100;
	 let str = '';
	 for (let i = 0; i < length; i++) {
		 str += String.fromCharCode(Math.random() * 1000 + 35);
	 }
	 return str;
}