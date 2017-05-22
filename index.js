const MongoOplog = require('mongo-oplog');
const { MongoClient, Timestamp } = require('mongodb');
const EventEmitter = require('events');

const opLogEE = new EventEmitter();

const config = {
	uri: 'mongodb://localhost:27018/test',
	oplogUri: 'mongodb://localhost:27018/local',
	oplogColl: 'oplog.rs',
	versionAppliedColl: 'versionApplied',
	collections: [{name: 'patients'}],
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
					coll.lowerBound = data[0].oplog_ts; // lower bound to last applied
				}
				else {
					coll.lowerBound = 0; // lower bound to all the previous data
				}
			});
	}));
}

opLogEE.on('insert', data => {
	console.log('insert op', data);
	const coll = data.ns.split('.')[1];
	const obj = data.o || {};
	obj.doc_id = obj._id;
	delete obj._id;
	obj.versioning_ts = data.ts;
	db.collection(`${config.prefix}${coll}`).insertOne(obj).then(result => console.log(result));
});

opLogEE.on('update', async data => {
	console.log('update op', data);
	const coll = data.ns.split('.')[1];
	const query = data.o2;
	const doc = await db.collection(`${config.prefix}${coll}`).findOne(query);
	const obj = data.o || {};
	doc.oplog_ts = data.ts;

	// obj might have three forms: 
	// $set: {<field>: <value>}, 
	// $unset: {<field>: <value>}, 
	// <field>: <value>
	// warning: array notation/ dot notation
	const $set = obj.$set;
	const $unset = obj.$unset;
	if ($set) {
		resolveField(doc, $set);
	}
	console.log(doc);
});

function resolveField(doc, key_value) {
	for (let key in key_value) {
		let chain = key.split('.');
		if (chain.length === 1) {
			if (Array.isArray(doc) && typeof chain[0] === 'number') {
				doc.splice(chain[0], 0, key_value[key]);
			} else {
				doc[chain[0]] = key_value[key];
			}
			continue;
		} else {
			if (!doc[chain[0]]) {
				if (typeof chain[1] === 'number') {
					doc[chain[0]] = [];
				} else {
					doc[chain[0]] = {};
				}
			}
			resolveField(doc[chain[0]], {[chain.slice(1).join('.')]: key_value[key]});
		}
	}
}

opLogEE.on('delete', data => {
	console.log('delete op', data)
	const coll = data.ns.split('.')[1];
	const obj = data.o || {};
	obj.doc_id = obj._id;
	delete obj._id;
	obj.versioning_ts = data.ts;
	obj.oplog_delete = true;
	db.collection(`${config.prefix}${coll}`).insertOne(obj).then(result => console.log(result));
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
}

function counstructTailingQuery() {
	const query = {$or: []};
	config.collections.forEach(coll => {
		query.$or.push({ns: `test.${coll.name}`, ts: {$gt: Timestamp(coll.lowerBound, 0)}})
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
