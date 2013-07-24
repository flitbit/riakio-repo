'use strict';

var riakio = require('riakio')
, metastore = require('./metastore')
, util = require('util')
;

function bindPipeline (funcs, bindTo) {
	
	var result = { before: null, on: null, after: null };

	if(funcs.before && funcs.before.length) {
		result.before = funcs.before.map(function(fn) { return fn.bind(bindTo); });
	}
	if(funcs.on && funcs.on.length) {
		result.on = funcs.on.map(function(fn) { return fn.bind(bindTo); });
	}
	if(funcs.after && funcs.after.length) {
		result.after = funcs.after.map(function(fn) { return fn.bind(bindTo); });
	}

	return result;
}

function executePipeline (input, before, main, on, after, callback) {
	var output, inputs = input
	;
	if(before) {
		before.forEach(function(fn) {
			fn(inputs, function(err, res) {
				if(err) { 
					callback(err);
					return;
				} else {
					inputs = res;
				}
			});
		});
	}
	main(inputs, function(err, res) {
		if(err) { 
			callback(err);
			return;
		} else {
			output = res;
		}
	});
	if(on) {
		on.forEach(function(fn) {
			try { 
				fn(output) 
			} catch(err) {
				// thanks observer... should log these I guess...
			}
		});
	}
	if(after) {
		after.forEach(function(fn) {
			fn(output, function(err, res) {
				if(err) { 
					callback(err);
					return;
				} else {
					output = res;
				}
			});
		});
	}

	callback(null, output);
}

function Repo (options) {
	
	var _bucket = riakio.Bucket.create(options.bucket)
	, _meta = metastore.loadMetadataForBucket(options);
	;

	Object.defineProperties(this, {
		name: { 
			get: function get_name() { return _bucket.name; }
			, enumerable: true 
		}
		, bucket: { value: _bucket }
		, metadata: { value: _meta }
		}
	});
}

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
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			var createPipeline
			;
			if(this.meta && this.meta.createPipeline) {
				createPipeline = bindPipeline(this.meta.createPipeline, this);
			}

			executePipeline(kv
				, createPipeline.before
				, function(inputs, cb) {
					try {
						this.bucket.createJsonItem(inputs.value, inputs.key).save(cb);
					} catch(err) {
						cb(err);
					}
				}
				, createPipeline.on
				, createPipeline.after
				, callback);
		}
		, enumerable: true
	}
	, update: { 
		value: function update(kv, callback) {
			dbc([kv && typeof kv === 'object'], 'kv (argument 0) must be an object');
			dbc([kv.value && typeof kv.value === 'object'], 'kv.value (argument 0) must be an object with a #value property');
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			var updatePipeline
			;
			if(this.meta && this.meta.updatePipeline) {
				updatePipeline = bindPipeline(this.meta.updatePipeline, this);
			}

			executePipeline(kv
				, updatePipeline.before
				, function(inputs, cb) {
					try {
						this.bucket.createJsonItem(inputs.value, inputs.key).save(cb);
					} catch(err) {
						cb(err);
					}
				}
				, updatePipeline.on
				, updatePipeline.after
				, callback);
		}
		, enumerable: true 
	}
	, delete: { 
		value: function delete(kv, callback) {
			dbc([kv && typeof kv === 'object'], 'kv (argument 0) must be an object');
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');
			
			var deletePipeline
			;
			if(this.meta && this.meta.deletePipeline) {
				deletePipeline = bindPipeline(this.meta.deletePipeline, this);
			}

			executePipeline(kv
				, deletePipeline.before
				, function(inputs, cb) {
					try {
						k = (inputs.key) ? (inputs.key || bucket.calculateKey(inputs.value)) : bucket.calculateKey(inputs.value);
						bucket.items.remove(k, cb);
					} catch(err) {
						cb(err);
					}
				}
				, deletePipeline.on
				, deletePipeline.after
				, callback);
		}
		, enumerable: true 
	}
	, get: {
		value: function get(key, callback) {
			dbc([key, key.length], 'key (argument 0) is required')
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			try {
				bucket.search.keys(riakio.KeyFilters.eq(key), callback);
			} catch (err) {
				callback(err);
			}
		}
		, enumerable: true 
	}
	, search: {
		value: function search(query, callback) {
			try {
				bucket.search.solr(query, callback);
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

			dbc([typeof cb === 'function'], 'callback (argument 3|4) must be a function');

			try {
				if(callback) {
					bucket.search.index(riakio.IndexFilter.range(val, end), cb);
				} else {
					bucket.search.index(riakio.IndexFilter.key(val), cb);
				}
			} catch(err) {
				cb(err);
			}
		}
	}
});

module.exports = Repo;