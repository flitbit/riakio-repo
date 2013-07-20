'use strict';

var riakio = require('riakio')
, metadata = require('./metadata')
, util = require('util')
;

function Repo (name, calculateKey, typeCtor) {
	dbc([typeof bucketName === 'string'], 'name (argument 1) should be a string');
	dbc([typeof calculateKey === 'function'], 'calculateKey (argument 2) must be a function')
	dbc([!typeCtor || typeof typeCtor === 'function'], 'typeCtor (argument 3) must be a function')

	var _bucket = riakio.Bucket({ bucket: bucketName, calculateKey: calculateKey })
	, _meta
	, _ctor = typeCtor ? function(shape) { return new typeCtor(shape); } : function(shape) { return shape; }
	;

	Object.defineProperties(this, {
		bucket: { value: _bucket }
		, ctor: { value: _ctor }
		, metadata: { get: function(){ return _meta; }}
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

Object.defineProperties(Repo.prototype, {
	create: { 
		value: function create(obj, key, callback) {
			var cb = callback || key
			;

			dbc([typeof cb === 'function'], 'callback (argument 2|3) must be a function');
			dbc([obj && typeof obj === 'object'], 'obj (argument 1) must be an object');

			if(meta.schema)	{
				//TODO validate 'obj' agaist the defined schema
			}

			if(meta.preCommitHooks.length)
			{
				var pre = -1
				;
				while(++pre < meta.preCommitHooks.length) {
					meta.preCommitHooks[pre](obj);
				}
			}

			this.bucket.createJsonItem(obj, key).save(function(err, res){
				if(meta.postCommitHooks.length)
				{
					var post = -1
					;
					while(++post < meta.postCommitHooks.length) {
						meta.postCommitHooks[post](obj);
					}
				}
				cb(err, res);
			});
		}
		, enumerable: true }
	, update: { 
		value: function update(obj, key, callback) {
			var cb = callback || key
			;

			dbc([typeof cb === 'function'], 'callback (argument 2|3) must be a function');
			dbc([obj && typeof obj === 'object'], 'obj (argument 1) must be an object');

			if(meta.preCommitHooks.length)
			{
				var pre = -1
				;
				while(++pre < meta.preCommitHooks.length) {
					meta.preCommitHooks[pre](obj);
				}
			}

			this.bucket.createJsonItem(obj, key).save(function(err, res){
				if(meta.postCommitHooks.length)
				{
					var post = -1
					;
					while(++post < meta.postCommitHooks.length) {
						meta.postCommitHooks[post](obj);
					}
				}
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

			dbc([typeof cb === 'function'], 'callback (argument 2|3) must be a function');
			
			k = (callback) ? (key || bucket.calculateKey(obj)) : bucket.calculateKey(obj);

			bucket.items.remove(k, callback);
		}
		, enumerable: true 
	}
	, get: {
		value: function get(key, callback) {
			dbc([key, key.length], 'key (argument 1) is required')
			dbc([typeof callback === 'function'], 'callback (argument 2) must be a function');

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
							callback(null, accum);
						} else {
							callback(null, ctor(r));
						}
					} catch(err) {
						callback(err);
					}
				}
			});
		}
		, enumerable: true 
	}
	, searchIndex: {
		value: function searchIndex(idx, val, end, callback) {
			dbc([typeof idx === 'string', idx.length], 'idx (argument 1) must be a string (json path, or index name)');
			dbc([val], 'val (argument 2) is required');

			var cb = callback || end
			;

			dbc([typeof cb === 'function'], 'callback (argument 3|4) must be a function');

			if(callback) {
				bucket.search.index(riakio.IndexFilter.range(val, end), cb);
			} else {
				bucket.search.index(riakio.IndexFilter.key(val), cb);
			}
		}
	}
});

module.exports = Repo;