'use strict';

var Hooked = require('Hooked').Hooked
, riakio = require('riakio')
, metadata = require('./metadata')
, util = require('util')
;

function Repo (name, calculateKey, typeCtor) {
	Repo.super_.call(this, { unhooked: ["validate"] });
	dbc([typeof bucketName === 'string'], 'name (argument 0) should be a string');
	dbc([typeof calculateKey === 'function'], 'calculateKey (argument 1) must be a function')
	dbc([!typeCtor || typeof typeCtor === 'function'], 'typeCtor (argument 2) must be a function')

	var _bucket = riakio.Bucket({ bucket: name, calculateKey: calculateKey })
	, _meta
	, _ctor = typeCtor ? function(shape) { return new typeCtor(shape); } : function(shape) { return shape; }
	;

	Object.defineProperties(this, {
		name: { value: name, enumerable: true }
		, bucket: { value: _bucket }
		, ctor: { value: _ctor }
		, metadata: { 
			get: function(){ return _meta; }
			, enumerable: true
		}
	});

	var that = this;
	_bucket.search.keys(riakio.KeyFilters.eq('_metadata'), function(err, res) {
		if(err) {
			console.log(util.inspect(err, false, 99));
			_meta = new metadata(that);
		} else if(res.result) { 
			_meta = new metadata(that, res.result); 
		}
	});
}
util.inherits(Repo, Hooked);

Object.defineProperties(Repo.prototype, {
	validate: {
		value: function validate(obj) {
			dbc([obj && typeof obj = 'object'], 'obj (arguement 0) must be an object')

			if(this.metadata) {
				return this.metadata.validate(obj);
			} else {
				return true; //No metadata, so nothing to validate on...
			}
		}
		, enumerable: true
	}
	, create: { 
		value: function create(kv, callback) {
			dbc([kv && typeof kv === 'object'], 'kv (argument 0) must be an object');
			dbc([kv.value && typeof kv.value === 'object'], 'kv.value (argument 0) must be an object with a #value property');
			dbc([typeof callback === 'function'], 'callback (argument 1|2) must be a function');

			this.bucket.createJsonItem(obj, key).save(function(err, res) {
				callback(err, res.result);
			});
		}
		, enumerable: true
	}
	, update: { 
		value: function update(obj, key, callback) {
			var cb = callback || key
			;

			dbc([obj && typeof obj === 'object'], 'obj (argument 0) must be an object');
			dbc([typeof cb === 'function'], 'callback (argument 1|2) must be a function');

			this.bucket.createJsonItem(obj, key).save(function(err, res) {
				cb(err, res);
			});
		}
		, enumerable: true 
	}
	, delete: { 
		value: function delete(obj, key, callback) {
			var cb = callback || key
			, k
			;

			dbc([typeof cb === 'function'], 'callback (argument 1|2) must be a function');
			
			try {
				k = (callback) ? (key || bucket.calculateKey(obj)) : bucket.calculateKey(obj);
				bucket.items.remove(k, cb);
			} catch(err) {
				cb(err);
			}
		}
		, enumerable: true 
	}
	, get: {
		value: function get(key, callback) {
			dbc([key, key.length], 'key (argument 0) is required')
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			bucket.search.keys(riakio.KeyFilters.eq(key), function(err, res) { 
				if(err) {
					callback(err);
				} else {
					try {
						var r = ctor(res.result);
						callback(null, r);
					} catch (err) {
						callback(err);
					}
				}
			});
		}
		, enumerable: true 
	}
	, search: {
		value: function search(query, callback) {
			try {
				bucket.search.solr(query, function(err, res) {
					if(err) {
						callback(err);
					} else {
						try {
							var r = res.result
							, accum = []
							;
							if(Array.isArray(r)) {
								r.forEach(function(el) {
									accum.push(ctor(el));
								});
							} else {
								accum.push(ctor(r));
							}
							callback(null, accum);
						} catch(err) {
							callback(err);
						}
					}
				});
			} catch(err) {
				callback(err);
			}
		}
		, enumerable: true 
	}
	, searchIndex: {
		value: function searchIndex(idx, val, end, callback) {
			dbc([typeof idx === 'string', idx.length], 'idx (argument 0) must be a string (json path, or index name)');
			dbc([val], 'val (argument 1) is required');

			var cb = callback || end
			;

			dbc([typeof cb === 'function'], 'callback (argument 2|4) must be a function');

			var wrapper = function(err, res) {
				if(err) {
					cb(err);
				} else {
					try {
						var r = res.result
						, accum = []
						;
						if(Array.isArray(r)) {
							r.forEach(function(el) {
								accum.push(ctor(el));
							});
						} else {
							accum.push(ctor(r));
						}
						cb(null, accum);
					} catch(err) {
						cb(err);
					}
				}
			}

			try {
				if(callback) {
					bucket.search.index(riakio.IndexFilter.range(val, end), wrapper);
				} else {
					bucket.search.index(riakio.IndexFilter.key(val), wrapper);
				}
			} catch(err) {
				cb(err);
			}
		}
	}
});

module.exports = Repo;