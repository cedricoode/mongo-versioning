const MongoOplog = require('mongo-oplog');
const { MongoClient, Timestamp, ObjectID } = require('mongodb');
const EventEmitter = require('events');

const opLogEE = new EventEmitter();

const config = {
	uri: 'mongodb://localhost:27018/test',
	oplogUri: 'mongodb://localhost:27018/local',
	oplogColl: 'oplog.rs',
	versionAppliedColl: 'versionApplied',
	collections: [{name: 'measurements'}],
	prefix: 'history_',
}

let db;

/**
 * Called once connection is established.
 * Set oplog tailing starting point, strategy:
 * 1. check prefix_collection last insert point.
 * 2. if this insert point doesnt exist, go through initiation process, else go to 3.
 * 3. tail the log greater than this point for this collection
 */
function getResumePoint(db) {
	return Promise.all(config.collections.map(coll => {
		const c = db.collection(`${config.prefix}${coll.name}`)
		return c.find().sort({versioning_ts: -1, _id: -1}).limit(1).toArray()
			.then(data => {
				if (data && data.length > 0) {
					coll.lowerBound = data[0].versioning_ts; // lower bound to last applied
				}
				else {
					coll.lowerBound = Timestamp(0, 0); // lower bound to all the previous data
				}
			});
	}));
}

opLogEE.on('insert', data => {
	console.log('insert op');
	const coll = data.ns.split('.')[1];
	const obj = data.o || {};
	obj.doc_id = obj._id;
	delete obj._id;
	obj.versioning_ts = data.ts;
	db.collection(`${config.prefix}${coll}`).insertOne(obj);
});

opLogEE.on('update', async data => {
	console.log('update op');
	const collName = data.ns.split('.')[1];
	
	const query = data.o2;
	// map _id to doc_id
	if (query._id) {
		query.doc_id = query._id;
		delete query._id;
	}

	const doc = await db.collection(`${config.prefix}${collName}`)
		.find(query)
		.sort({_id: -1})
		.limit(1)
		.toArray()
		.then(data => data[0]);
	doc.versioning_ts = data.ts;

	const update = data.o || {};

	replayUpdate(doc, update);
	
	await db.collection(`${config.prefix}${collName}`).insertOne(doc);
});

function replayUpdate(doc, update) {
	const $set = update.$set;
	const $unset = update.$unset;
	if ($set) {
		setField(doc, $set);
	}
	if ($unset) {
		unsetField(doc, $unset);
	}
	delete update.$set;
	delete update.$unset;
	delete update._id;
	setField(doc, update);
	delete doc._id;
}

function unsetField(doc, key_value) {
	for (let key in key_value) {
		let chain = key.split('.');
		chain.reduce((acc, cur, ind, arr) => {
			if (!acc) {
				return null;
			}
			if (ind === arr.length - 1) {
				delete acc[cur];
			}
			return acc[cur];
		}, doc)
	}
}

function setField(doc, key_value) {
	for (let key in key_value) {
		let chain = key.split('.');
		if (chain.length === 1) {
			if (Array.isArray(doc) && !isNaN(parseInt(chain[0]))) {
				doc.splice(chain[0], 0, key_value[key]);
			} else {
				doc[chain[0]] = key_value[key];
			}
			continue;
		} else {
			if (!doc[chain[0]]) {
				if (!isNaN(parseInt(chain[1]))) {
					doc[chain[0]] = [];
				} else {
					doc[chain[0]] = {};
				}
			}
			setField(doc[chain[0]], {[chain.slice(1).join('.')]: key_value[key]});
		}
	}
}

opLogEE.on('delete', data => {
	//console.log('delete op', data)
	const coll = data.ns.split('.')[1];
	const obj = data.o || {};
	obj.doc_id = obj._id;
	delete obj._id;
	obj.versioning_ts = data.ts;
	obj.oplog_delete = true;
	db.collection(`${config.prefix}${coll}`).insertOne(obj);
});

opLogEE.on('error', err => {
	console.log('oplog event emitter error: ', err);
});

async function init(configuration) {
	// Override local configuration
	if (configuration) {
		for (let i in config) {
			if (configuration[i]) {
				config[i] = configuration[i];
			}
		}
	}
	// Connect to targetting database
	db = await MongoClient.connect(config.uri);
	
	// Setup resume point
	await getResumePoint(db);
	console.log('resume point: ', config.collections);
}

function counstructTailingQuery() {
	const query = {$or: []};
	config.collections.forEach(coll => {
		query.$or.push({ns: `test.${coll.name}`, ts: {$gt: coll.lowerBound}})
	});
	return query;
}

async function start() {
	// Construct tailing query
	const query = counstructTailingQuery();
	var db = await MongoClient.connect(config.oplogUri);
	var oplog = db.collection('oplog.rs');
	st = oplog.find(query, {
		awaitData: true, 
		tailable: true,
		noCursorTimeout: true,
		oplogReplay: true
	}).stream();
	st.on('data', data => {
		console.log('new data');
		switch(data.op) {
			case 'i':
				opLogEE.emit('insert', data);
				break;
			case 'u':
				opLogEE.emit('update', data);
				break;
			case 'd':
				opLogEE.emit('delete', data)
				break;
			default:
				opLogEE.emit('op', data);
				break;
		}
	});
	st.on('error', err => {
		console.log(err)
		st.removeAllListener();
	});
	st.on('end', () => {
		st.removeAllListener();
		console.log('ended')
	});
	return opLogEE;
}

function stop() {

}

module.exports = {
	init,
	start,
	stop,
}
