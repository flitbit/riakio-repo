'use strict';

var riakio = require('riakio')
, metastore = require('./metastore')
, util = require('util')
;

function Repo(options) {
	
	var _bucket = riakio.Bucket.create(options.bucket)
	, _meta
	, _name = _bucket.name
	, _log = options.log
	;

	Object.defineProperties(this, {
		 bucket: { value: _bucket }
		, metadata: { value: _meta }
		, log: { value: _log }
		, name: { 
			get: function get_name() { return _name; }
			, enumerable: true 
		}
	});

	metastore.loadMetadataForBucket(options, function(err, res) {
		if(err) {
			if(log && log.error) {
				log.error(util.inspect(err, true, 99));
			}
		} else {
			_meta = res;
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

			var createPipeline = { before: null, on: null, after: null }
			;
			if(this.meta && this.metadata.createPipeline) {
				createPipeline = this.bindPipeline(this.metadata.createPipeline);
			}

			this.executePipeline(kv
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
			if(this.meta && this.metadata.updatePipeline) {
				updatePipeline = this.bindPipeline(this.metadata.updatePipeline);
			}

			this.executePipeline(kv
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
		value: function del(kv, callback) {
			dbc([kv && typeof kv === 'object'], 'kv (argument 0) must be an object');
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');
			
			var deletePipeline
			;
			if(this.meta && this.metadata.deletePipeline) {
				deletePipeline = this.bindPipeline(this.metadata.deletePipeline);
			}

			this.executePipeline(kv
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
	, bindPipeline: { 
		value: function bindPipeline(funcs) {
	
			var result = { before: null, on: null, after: null };

			if(funcs.before && funcs.before.length) {
				result.before = funcs.before.map(function(fn) { return fn.bind(this); });
			}
			if(funcs.on && funcs.on.length) {
				result.on = funcs.on.map(function(fn) { return fn.bind(this); });
			}
			if(funcs.after && funcs.after.length) {
				result.after = funcs.after.map(function(fn) { return fn.bind(this); });
			}

			return result;
		}
	}
	, executePipeline: {
		value: function executePipeline(input, before, main, on, after, callback) {
			var inputs = input
			;
			if(before) {
				before.forEach(function(fn) {
					try {
						fn(inputs, function(err, res) {
							if(err) { 
								callback(err);
								return;
							} else {
								inputs = res;
							}
						});
					} catch (e) {
						callback(e);
					}
				});
			}
			try {
				main(inputs, function(err, res) {
					if(err) { 
						callback(err);
						return;
					} else {
						var output = res;

						if(on) {
							on.forEach(function(fn) {
								process.nextTick(function() {
									try { 
										 fn(output);
									} catch(e) {
										if(this.log && this.log.error) {
											this.log.error(util.inspect(e, true, 99));
										}
									}
								});
							});
						}
						if(after) {
							try {
								after.forEach(function(fn) {
									fn(output, function(e, res) {
										if(e) { 
											callback(e);
											return;
										} else {
											output = res;
										}
									});
								});
							} catch(e) {
								callback(e);
							}
						}

						callback(null, output);
					}
				});
			} catch (e) {
				callback(e);
			}
		}
	}
});

module.exports = Repo;