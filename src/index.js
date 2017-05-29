const { Timestamp, ObjectID } = require('mongodb');
/**
 * Called once connection is established.
 * Set oplog tailing starting point, strategy:
 * 1. check prefix_collection last insert point.
 * 2. if this insert point doesnt exist, go through initiation process, else go to 3.
 * 3. tail the log greater than this point for this collection
 */
function getResumePoint(db, config) {
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

function counstructTailingQuery(config) {
	const query = {$or: []};
	const p = config.uri.split('/'); // Assume db uri last part is the database name
	const db = p[p.length - 1];
	config.collections.forEach(coll => {
		query.$or.push({ns: `${db}.${coll.name}`, ts: {$gt: coll.lowerBound}})
	});
	return query;
}

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

module.exports = {
	getResumePoint,
	replayUpdate,
	counstructTailingQuery
}