'use strict';

var riakio = require('riakio')
, EventEmitter = require('events').EventEmitter
, dbc = require('dbc.js')
, webflow = require('webflow')
, Success = webflow.Success
, ResourceError = webflow.ResourceError
, metastore = require('./metastore')
, util = require('util')
;

function Repo(options) {
	Repo.super_.call(this);
	var _options = options
	, _bucket = riakio.Bucket.create(options)
	, _meta
	, _name = _bucket.name
	, _log = options.log
	;

	Object.defineProperties(this, {
		 bucket: { value: _bucket }
		, log: { value: _log }
		, name: { 
			get: function get_name() { return _name; }
			, enumerable: true 
		}
		, meta: {
			get: function get_meta() { return _meta; }
			, set: function set_meta(val) { _meta = val; }
		}
		, options: {
			value: _options
		}
	});

	this.on('error', function(err){
		if(_log) {
			_log.error('error', 'Repo threw error: '.concat(util.inspect(err, true, 99)));
			if(err.stack) {
				_log.error(err.stack);
			}
		}
	})
}
util.inherits(Repo, EventEmitter);

Object.defineProperties(Repo.prototype, {
	 metadata: { 
		value: function(callback) {
			if (this.meta) {
				callback(null, this.meta);
			} else {
				var that = this
				;
				metastore.getMetadataForBucket(this.options, function(err, res) {
					if(err) {
						callback(err);
					} else {
						that.meta = res;
						callback(null, that.meta);
					}
				});
			}
		}
	}
	, validate: {
		value: function validate(obj, callback) {
			dbc([obj && typeof obj = 'object'], 'obj (arguement 0) must be an object')

			this.metadata(function(err, res) {
				if (err) {
					callback(err);
				} else {
					if (res.result.validate(obj)) {
						callback(null, Success.ok);
					} else {
						callback(ResourceError.unprocessableEntity);
					}
				}
			});
		}
		, enumerable: true
	}
	, create: {
		value: function create(kv, callback) {
			dbc([kv && typeof kv === 'object'], 'kv (argument 0) must be an object');
			dbc([kv.value && typeof kv.value === 'object'], 'kv.value (argument 0) must be an object with a #value property');
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			var that = this
			;
			this.metadata(function(err, res) {
				if(err) {
					callback(err);
				} else {
					var meta = res.result
					;
					
					that.__write(kv
						, meta.createPipeline
						, function(inputs, cb) {
							try {
								that.bucket.createJsonItem(inputs.value, inputs.key).save(cb);
							} catch(err) {
								cb(err);
							}
						}
						, callback);
				}
			});
		}
		, enumerable: true
	}
	, createMany: {
		value: function createMany(kva, callback) {
			dbc([kva && Array.isArray(kva)], 'kva (argument 0) must be an array');
			dbc([kva[0].value && typeof kva[0].value === 'object'], 'kv[0].value (argument 0) must be an object with a #value property');
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			var that = this
			;
			this.metadata(function(err, res) {
				if(err) {
					callback(err);
				} else {
					var meta = res.result
					;
					that.__writeMany(kva
						, meta.createPipeline
						, function(inputs, cb) {
							try {
								that.bucket.createJsonItem(inputs.value, inputs.key).save(cb);
							} catch(err) {
								cb(err);
							}
						}
						, callback);
				}
			});
		}
	}
	, update: { 
		value: function update(kv, callback) {
			dbc([kv && typeof kv === 'object'], 'kv (argument 0) must be an object');
			dbc([kv.value && typeof kv.value === 'object'], 'kv.value (argument 0) must be an object with a #value property');
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			var that = this
			;
			this.metadata(function(err, res) {
				if(err) {
					callback(err);
				} else {
					var meta = res.result
					;
					that.__write(kv
						, meta.updatePipeline
						, function(inputs, cb) {
							try {
								that.bucket.createJsonItem(inputs.value, inputs.key).save(cb);
							} catch(err) {
								cb(err);
							}
						}
						, callback);
				}
			});
		}
		, enumerable: true 
	}
	, updateMany: {
		value: function updateMany(kva, callback) {
			dbc([kva && Array.isArray(kva)], 'kva (argument 0) must be an array');
			dbc([kva[0].value && typeof kva[0].value === 'object'], 'kv[0].value (argument 0) must be an object with a #value property');
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			var that = this
			;
			this.metadata(function(err, res) {
				if(err) {
					callback(err);
				} else {
					var meta = res.result
					;
					that.__writeMany(kva
						, meta.updatePipeline
						, function(inputs, cb) {
							try {
								that.bucket.createJsonItem(inputs.value, inputs.key).save(cb);
							} catch(err) {
								cb(err);
							}
						}
						, callback);
				}
			});
		}
	}
	, delete: { 
		value: function del(kv, callback) {
			dbc([kv && typeof kv === 'object'], 'kv (argument 0) must be an object');
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');
			
			var that = this
			;
			this.metadata(function(err, res) {
				if(err) {
					callback(err);
				} else {
					var meta = res.result
					;
			
					that.__write(kv
						, meta.deletePipeline
						, function(inputs, cb) {
							try {
								var k = (inputs.key) ? (inputs.key || that.bucket.calculateKey(inputs.value)) : that.bucket.calculateKey(inputs.value);
								that.bucket.items.remove(k, cb);
							} catch(err) {
								cb(err);
							}
						}
						, callback);
				}
			});
		}
		, enumerable: true 
	}
	, deleteMany: {
		value: function deleteMany(kva, callback) {
			dbc([kva && Array.isArray(kva)], 'kva (argument 0) must be an array');
			dbc([kva[0] && (typeof kva[0].value === 'object' || typeof kva[0].key === 'string')], 'kva (argument 0) must have elements with #value (object) OR #key (string) properties');
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			var that = this
			;
			this.metadata(function(err, res) {
				if(err) {
					callback(err);
				} else {
					var meta = res.result
					;
			
					that.__writeMany(kva
						, meta.deletePipeline
						, function(inputs, cb) {
							try {
								var k = (inputs.key) ? (inputs.key || that.bucket.calculateKey(inputs.value)) : that.bucket.calculateKey(inputs.value);
								that.bucket.items.remove(k, cb);
							} catch(err) {
								cb(err);
							}
						}
						, callback);
				}
			});
		}
	}
	, get: {
		value: function get(key, callback) {
			dbc([key, key.length], 'key (argument 0) is required')
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			try {
				this.bucket.items.fetch(key, callback);
			} catch (err) {
				callback(err);
			}
		}
		, enumerable: true 
	}
	, search: {
		value: function search(query, callback) {
			try {
				this.bucket.search.solr(query, callback);
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

			dbc([typeof cb === 'function'], 'callback (argument 2|3) must be a function');

			try {
				if(callback) {
					this.bucket.search.index(riakio.IndexFilter.range(val, end), cb);
				} else {
					this.bucket.search.index(riakio.IndexFilter.key(val), cb);
				}
			} catch(err) {
				cb(err);
			}
		}
	}
	, __write: {
		value: function __write(kv, pipe, main, callback) {
			var pipeline = this.bindPipeline(pipe)
			;
					
			this.executePipeline(kv
				, pipeline.before
				, main
				, pipeline.on
				, pipeline.after
				, callback);
		}
	}
	, __writeMany: {
		value: function __writeMany(kva, pipe, main, callback) {
			var pipeline = this.bindPipeline(pipe)
			, kv
			, kvi = -1
			;
			while (++kvi < kva.length) {
				kv = kva[kvi];
				this.executePipeline(kv
					, pipeline.before
					, main
					, pipeline.on
					, pipeline.after
					, callback);
			}
		}
	}
	, bindPipeline: { 
		value: function bindPipeline(funcs) {
	
			var result = { before: null, on: null, after: null }
			, that = this
			;

			if(funcs.before && funcs.before.length) {
				result.before = funcs.before.map(function(fn) { return fn.bind(that); });
			}
			if(funcs.on && funcs.on.length) {
				result.on = funcs.on.map(function(fn) { return fn.bind(that); });
			}
			if(funcs.after && funcs.after.length) {
				result.after = funcs.after.map(function(fn) { return fn.bind(that); });
			}

			return result;
		}
	}
	, executePipeline: {
		value: function executePipeline(input, before, main, on, after, callback) {
			var inputs = input
			, ex
			;
			if(before) {
				var bi = -1
				;
				while (++bi < before.length && !ex) {
					try {
						var fn = before[bi];
						fn(inputs, function(err, res) {
							if(err) { 
								ex = err;
							} else {
								inputs = res;
							}
						});
					} catch (e) {
						ex = e;
					}
				}
				if(ex) {
					callback(ex);
					return;
				}
			}
			try {
				var that = this
				;
				main(inputs, function(err, res) {
					if(err) { 
						callback(err);
					} else {
						var output = res;

						if(on) {
							on.forEach(function(fn) {
								process.nextTick(function() {
									try { 
										 fn(output);
									} catch(e) {
										that.emit('error', e);
									}
								});
							});
						}
						if(after) {
							var ai = -1
							;
							while (++ai < after.length && !ex) {
								try {
									var fn = after[ai]
									;
									fn(output, function(err, res) {
										if(err) { 
											ex = err;
										} else {
											output = res;
										}
									});
								} catch(e) {
									ex = e
								}
							}
						}
						if(ex) {
							callback(ex);
						} else {
							callback(null, output);
						}
					}
				});
			} catch (e) {
				callback(e);
			}
		}
	}
});

module.exports = Repo;