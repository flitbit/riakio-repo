'use strict';

var riakio = require('riakio')
, dbc = require('dbc.js')
, webflow = require('webflow')
, Success = webflow.Success
, ResourceError = webflow.ResourceError
, metastore = require('./metastore')
, util = require('util')
;

function Repo(options) {
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
}

Object.defineProperties(Repo.prototype, {
	 metadata: { 
		value: function(callback) {
			if (this.meta) {
				callback(null, this.meta);
			} else {
				metastore.getMetadataForBucket(this.options, function(err, res) {
					if(err) {
						callback(err);
					} else {
						this.meta = res;
						callback(null, this.meta);
					}
				}.bind(this));
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
					, createPipeline = that.bindPipeline(meta.createPipeline)
					;
					
					that.executePipeline(kv
						, createPipeline.before
						, function(inputs, cb) {
							try {
								that.bucket.createJsonItem(inputs.value, inputs.key).save(cb);
							} catch(err) {
								cb(err);
							}
						}
						, createPipeline.on
						, createPipeline.after
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
					var kvi = -1
					;
					while (++kvi < kva.length) {
						that.create(kva[kvi], callback);
					}
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
					, updatePipeline = that.bindPipeline(meta.updatePipeline);
			
				that.executePipeline(kv
					, updatePipeline.before
					, function(inputs, cb) {
						try {
							that.bucket.createJsonItem(inputs.value, inputs.key).save(cb);
						} catch(err) {
							cb(err);
						}
					}
					, updatePipeline.on
					, updatePipeline.after
					, callback);
				}
			});
		}
		, enumerable: true 
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
					, deletePipeline = that.bindPipeline(meta.deletePipeline);
			
					that.executePipeline(kv
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
			});
		}
		, enumerable: true 
	}
	, get: {
		value: function get(key, callback) {
			dbc([key, key.length], 'key (argument 0) is required')
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			try {
				bucket.items.fetch(key, callback);
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

			dbc([typeof cb === 'function'], 'callback (argument 2|3) must be a function');

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
				while (++i < before.length && !ex) {
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
						ex = err;
					} else {
						var output = res;

						if(on) {
							on.forEach(function(fn) {
								process.nextTick(function() {
									try { 
										 fn(output);
									} catch(e) {
										if(that.log && that.log.error) {
											that.log.error(util.inspect(e, true, 99));
										}
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
							if(ex) {
								callback(ex);
								return;
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