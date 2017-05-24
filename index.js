const { MongoClient } = require('mongodb');
const EventEmitter = require('events');
const { PassThrough } = require('stream');
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
	});
	this.streams = {};
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
	let count = 0;
	this.mongoEE.on('data', data => {
		console.log(`new data: ${count++}: `, data.ns);		
		const collName = data.ns.split('.')[1];
		if (!this.streams[collName]) {
			this.setUpStream(collName);
		}
		const writeOk = this.streams[collName].write(data); // relate to highWatermark
		if (!writeOk) {
			this.mongoEE.pause();
			this.streams[collName].once('drain',
				() => {
					this.mongoEE.resume();
					this.streams[collName].write(data)
				}
			);
		}
	});
	this.mongoEE.on('error', err => {
		handleError(err);
	});
	this.mongoEE.on('close', () => {
		console.log('Mongo cursor: cursor closed!');
	});
	this.mongoEE.on('end', () => {
		console.log('Mongo cursor: no more data here!');
	})
}

/**
 * Set up a stream to listen for collection data, this stream will dispatch 
 * relevant event data to event handlers and consume the data one by one.
 */
MongoVersioning.prototype.setUpStream = function setUpStream(key) {
	let count = 0;
	const passThrough = new PassThrough({readableObjectMode: true, writableObjectMode: true});
	passThrough.on('data', data => {
		passThrough.pause();
		switch(data.op) {
			case 'i':
				this.oplogEE.emit('insert', data, count++, passThrough);
				break;
			case 'u':
				this.oplogEE.emit('update', data, count++, passThrough);
				break;
			case 'd':
				this.oplogEE.emit('delete', data, count++, passThrough);
				break;
			default:
				this.oplogEE.emit('op', data, count++, passThrough);
				break;
		}
	});
	passThrough.on('error', err => {
		console.log(`passThrough stream error, ${key}: `, err);
		handleError(err);
	});
	this.streams[key] = passThrough;
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

MongoVersioning.prototype.insertListener = function insertListener(data, count, strm) {
	const collName = data.ns.split('.')[1];
	const doc = data.o || {};
	doc.versioning_id = doc._id;
	delete doc._id;
	doc.versioning_ts = data.ts;
	doc.versioning_version = 0;
	this.db.collection(this.prefixedColl[collName]).insertOne(doc)
		.then(() => {console.log(`operation result for: ${collName}`, count); strm.resume();});
}

MongoVersioning.prototype.updateListener = async function updateListener(data, count, strm) {
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
	doc.versioning_version = doc.versioning_version + 1;
	await this.db.collection(this.prefixedColl[collName]).insertOne(doc)
		.then(() => {console.log(`operation result for: ${collName}`, count); strm.resume();});
}

MongoVersioning.prototype.deleteListener = function deleteListener(data, count, strm) {
		//console.log('delete op', data)
	const collName = data.ns.split('.')[1];
	const obj = data.o || {};
	obj.versioning_id = obj._id;
	delete obj._id;
	obj.versioning_ts = data.ts;
	obj.versioning_delete = true;
	this.db.collection(this.prefixedColl[collName]).insertOne(obj)
		.then(() => {console.log(`operation result for: ${collName}`, count); strm.resume();});
}

MongoVersioning.prototype.opListener = function opListener(data, count, strm) {
	console.log(`operation result for: ${data.op}`, count);
	strm.resume();
}

MongoVersioning.prototype.errListener = function errListener(data, count) {
	console.log('oplog event emitter error: ', err);	
}

function handleError(err) {
	console.log(err);
}

module.exports = MongoVersioning;
