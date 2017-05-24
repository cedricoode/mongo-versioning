const { MongoClient } = require('mongodb');
const EventEmitter = require('events');
const {
	getResumePoint,
	replayUpdate,
	counstructTailingQuery
} = require('./src');

const DEFAULT_CONFIG = {
	uri: 'mongodb://localhost:27018/test',
	oplogUri: 'mongodb://localhost:27018/local',
	oplogColl: 'oplog.rs',
	versionAppliedColl: 'versionApplied',
	collections: [{name: 'patients'}, {name: 'measurements'}],
	prefix: 'history_',
}

function MongoVersioning(config) {
	this.configuration = Object.assign({}, DEFAULT_CONFIG);
	if (config) {
		for (let i in this.configuration) {
			if (config[i]) {
				this.configuration[i] = config[i];
			}
		}
	}
	this.oplogEE = new EventEmitter();
	this.oplogEE.on('insert', this.insertListener.bind(this));
	this.oplogEE.on('update', this.updateListener.bind(this));
	this.oplogEE.on('delete', this.deleteListener.bind(this));
	this.oplogEE.on('op', this.opListener.bind(this));
	this.oplogEE.on('error', this.errListener.bind(this));
	this.prefixedColl = {};
	this.configuration.collections.forEach(coll => {
		this.prefixedColl[coll.name] = `${this.configuration.prefix}${coll.name}`;
	})
}

MongoVersioning.prototype.start = async function start() {
	// Construct tailing query
	const query = counstructTailingQuery(this.configuration);
	console.log('query is: ', JSON.stringify(query));
	this.oplogDb = await MongoClient.connect(this.configuration.oplogUri);
	var oplog = this.oplogDb.collection('oplog.rs');
	this.mongoEE = oplog.find(query, {
		awaitData: true, 
		tailable: true,
		noCursorTimeout: true,
	}).stream();
	var count = 1;
	this.mongoEE.on('data', data => {
		console.log('new data: ', data.ns);
		this.mongoEE.pause();
		switch(data.op) {
			case 'i':
				this.oplogEE.emit('insert', data, count++);
				break;
			case 'u':
				this.oplogEE.emit('update', data, count++);
				break;
			case 'd':
				this.oplogEE.emit('delete', data, count++);
				break;
			default:
				this.oplogEE.emit('op', data, count++);
				break;
		}
	});
	st.on('error', err => {
		console.log(err)
		st.removeAllListeners();
	}); 
}

MongoVersioning.prototype.init = async function init() {
	// Connect to targetting database
	this.db = await MongoClient.connect(this.configuration.uri);
	
	// Setup resume point
	await getResumePoint(this.db, this.configuration);
	console.log('resume point: ', this.configuration.collections);
	return this;
}

MongoVersioning.prototype.stop = function stop() {
	this.oplogEE.removeAllListeners();
}

MongoVersioning.prototype.insertListener = function insertListener(data, count) {
	console.log('insert op');
	const collName = data.ns.split('.')[1];
	const doc = data.o || {};
	doc.versioning_id = doc._id;
	delete doc._id;
	doc.versioning_ts = data.ts;
	this.db.collection(this.prefixedColl[collName]).insertOne(doc)
		.then(() => {console.log('operation result for: ', count); this.mongoEE.resume();});
}

MongoVersioning.prototype.updateListener = async function updateListener(data, count) {
	console.log('update op');
	const collName = data.ns.split('.')[1];
	
	const query = data.o2;
	// map _id to versioning_id
	if (query._id) {
		query.versioning_id = query._id;
		delete query._id;
	}

	const doc = await this.db.collection(this.prefixedColl[collName])
		.find(query)
		.sort({_id: -1})
		.limit(1)
		.toArray()
		.then(data => data[0]);
	doc.versioning_ts = data.ts;

	const update = data.o || {};

	replayUpdate(doc, update);
	
	await this.db.collection(this.prefixedColl[collName]).insertOne(doc)
		.then(() => {console.log('operation result for: ', count); this.mongoEE.resume();});
}

MongoVersioning.prototype.deleteListener = function deleteListener(data, count) {
		//console.log('delete op', data)
	const collName = data.ns.split('.')[1];
	const obj = data.o || {};
	obj.versioning_id = obj._id;
	delete obj._id;
	obj.versioning_ts = data.ts;
	obj.versioning_delete = true;
	this.db.collection(this.prefixedColl[collName]).insertOne(obj)
		.then(() => {console.log('operation result for: ', count); this.mongoEE.resume();});
}

MongoVersioning.prototype.opListener = function opListener(data, count) {
	console.log('operation result for: ', count);
	this.mongoEE.resume();
}

MongoVersioning.prototype.errListener = function errListener(data, count) {
	console.log('oplog event emitter error: ', err);	
}

module.exports = MongoVersioning;
