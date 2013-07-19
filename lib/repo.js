'use strict';

var riakio = require('riakio')
, metadata = require('./metadata')
, util = require('util')
;

function Repo (name, calculateKey) {
	dbc([typeof bucketName === 'string'], 'name (argument 0) should be a string');
	dbc([typeof calculateKey === 'function'], 'calculateKey (argument 1) must be a function')

	var _bucket = riakio.Bucket({ bucket: bucketName, calculateKey: calculateKey })
	, _meta
	;

	Object.defineProperties(this, {
		bucket: { value: _bucket }
		, metadata: { get: function(){ return _meta; }}
	});

	var that = this;
	_bucket.search.keys(riakio.KeyFilters.eq('_metadata'), function(err, res) {
		if(err) {
			console.log(util.inspect(err, true, 99));
			_meta = new metadata(that);
		} else if(res.result) { 
			_meta = new metadata(that, res.result); 
		}
	});
}

Object.defineProperties(Repo.prototype, {
	create: { 
		value: function create(callback, obj, key) {
			dbc([typeof callback !== 'function'], 'callback (argument 0) must be a function');
			dbc([obj && typeof obj !== 'object'], 'obj (argument 1) must be an object');

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
				callback(err, res);
			});
		}
		, enumerable: true }
	, update: { 
		value: function update(callback, obj, key) {
			dbc([typeof callback !== 'function'], 'callback (argument 0) must be a function');
			dbc([obj && typeof obj !== 'object'], 'obj (argument 1) must be an object');

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
				callback(err, res);
			});
		}
		, enumerable: true 
	}
	, delete: { 
		value: function delete(callback, obj, key) {
			dbc([typeof callback !== 'function'], 'callback (argument 0) must be a function');
			
			var k = key || bucket.calculateKey(obj)
			;

			bucket.items.remove(k, callback);
		}
		, enumerable: true 
	}
	, get: {
		value: function get(callback, key) {
			dbc([typeof callback !== 'function'], 'callback (argument 0) must be a function');
			dbc([key, key.length], 'key (argument 1) is required')

			bucket.search.keys(riakio.KeyFilters.eq(key), callback);
		}
		, enumerable: true 
	}
});

module.exports = Repo;