'use strict';

var riakio = require('riakio')
, webflow = require('webflow')
, Success = webflow.Success
, ResourceError = webflow.ResourceError
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
		, log: { value: _log }
		, name: { 
			get: function get_name() { return _name; }
			, enumerable: true 
		}
		, metadata: { 
			value: function(callback) {
				if (_meta) {
					callback(null, _meta);
				}
				else {
					metastore.loadMetadataForBucket(options, function(err, res) {
						if(err) {
							callback(err);
						} else {
							_meta = res;
							callback(null, _meta);
						}
					});
				}
			}
		}
	});
}

Object.defineProperties(Repo.prototype, {
	validate: {
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

			this.metadata(function(err, res) {
				if(err) {
					callback(err);
				} else {
					var meta = res.result
					, createPipeline = this.bindPipeline(meta.createPipeline)
					;
					
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
			});
		}
		, enumerable: true
	}
	, update: { 
		value: function update(kv, callback) {
			dbc([kv && typeof kv === 'object'], 'kv (argument 0) must be an object');
			dbc([kv.value && typeof kv.value === 'object'], 'kv.value (argument 0) must be an object with a #value property');
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			this.metadata(function(err, res) {
				if(err) {
					callback(err);
				} else {
					var meta = res.result
					, updatePipeline = this.bindPipeline(meta.updatePipeline);
			
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
			});
		}
		, enumerable: true 
	}
	, delete: { 
		value: function del(kv, callback) {
			dbc([kv && typeof kv === 'object'], 'kv (argument 0) must be an object');
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');
			
			this.metadata(function(err, res) {
				if(err) {
					callback(err);
				} else {
					var meta = res.result
					, deletePipeline = this.bindPipeline(meta.deletePipeline);
			
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
										if(this.log && this.log.error) {
											this.log.error(util.inspect(e, true, 99));
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