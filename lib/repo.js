'use strict';

var riakio      = require('riakio')
, EventEmitter  = require('events').EventEmitter
, dbc           = require('dbc.js')
, webflow       = require('webflow')
, Success       = webflow.Success
, ResourceError = webflow.ResourceError
, metastore     = require('./metastore')
, util          = require('util')
;

function StreamingRepoOp(items, repo, op) {
	StreamingRepoOp.super_.call(this);
	var _items = Array.isArray(items) ? items : [items]
	, _repo = repo
	, _op = op
	, _cancelledEmitted = false
	, _doneEmitted = false
	;

	Object.defineProperties(this, {

		repo: {
			value: _repo
			, enumerable: true
		},

		op: {
			value: _op
			, enumerable: true
		},

		items: {
			value: _items
			, enumerable: true
		},

		cancelled: {
			value: _cancelledEmitted
			, enumerable: true
		},

		completed: {
			value: _doneEmitted
			, enumerable: true
		},

		cancelledEmitted: {
			get: function get_cancelledEmitted() { return _cancelledEmitted; }
			, set: function set_cancelledEmitted(val) { _cancelledEmitted = val; }
		},

		doneEmitted: {
			get: function get_doneEmitted() { return _doneEmitted; }
			, set: function set_doneEmitted(val) { _doneEmitted = val; }
		}

	});
}
util.inherits(StreamingRepoOp, EventEmitter);

Object.defineProperties(StreamingRepoOp.prototype, {

	exec: {
		value: function exec() {
			var todo = this.items.length
			, len = this.items.length
			, i = -1
			, that = this
			, item
			;

			if(!this.cancelled) {
				that.repo.metadata(function(err, res) {
					if(err) {
						--todo;
						that.emit('data', err, { item: item, result: res });
						if(todo <= 0) {
							that.doneEmitted = true;
							that.emit('done');
						}
					} else {
						while (++i < len)
						{
							item = that.items[i];

							if(that.cancelled) {
								break;
							} else {
								if(!that.cancelled) {
									that.repo[that.op](item, function(err, res) {
										--todo;
										that.emit('data', err, { item: item, result: res });
										if(todo <= 0) {
											that.doneEmitted = true;
											that.emit('done');
										}
									});
								}
							}
						}
					}
				});
			} else {
				var e = new Error('Operation has already been cancelled!');
				this.emit('error', e);
				throw e;
			}
		}
		, enumerable: true
	},

	cancel: {
		value: function cancel () {
			if(!this.completed && !this.cancelledEmitted) {
				this.cancelledEmitted = true;
				this.emit('cancelled');
			}
		}
		, enumerable: true
	}

});

function Repo(options) {
	Repo.super_.call(this);
	var _options = options
	, _bucket = riakio.Bucket.create(options)
	, _meta
	, _createPipeline = { before: [], on: [], after: [] }
	, _updatePipeline = { before: [], on: [], after: [] }
	, _deletePipeline = { before: [], on: [], after: [] }
	, _name = _bucket.name
	, _log = options.log
	;

	Object.defineProperties(this, {

		name: {
			get: function get_name() { return _name; }
			, enumerable: true
		},

		bucket: { value: _bucket },

		log: { value: _log },

		meta: {
			get: function get_meta() { return _meta; }
			, set: function set_meta(val) { _meta = val; }
		},

		createPipeline: {
			get: function get_createPipeline() { return _createPipeline; }
			, set: function set_createPipeline(val) { _createPipeline = val; }
		},

		updatePipeline: {
			get: function get_updatePipeline() { return _updatePipeline; }
			, set: function set_updatePipeline(val) { _updatePipeline = val; }
		},

		deletePipeline: {
			get: function get_deletePipeline() { return _deletePipeline; }
			, set: function set_deletePipeline(val) { _deletePipeline = val; }
		},

		options: {
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
						that.createPipeline = that.bindPipeline(that.meta.result.createPipeline);
						that.updatePipeline = that.bindPipeline(that.meta.result.updatePipeline);
						that.deletePipeline = that.bindPipeline(that.meta.result.deletePipeline);
						callback(null, that.meta);
					}
				});
			}
		}
	},

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
	},

	create: {
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
					if(meta.validate && !meta.validate(kv.item))
					{
						callback(ResourceError.unprocessableEntity);
					} else {
						that.__write(kv
							, that.createPipeline
							, function(inputs, cb) {
								try {
									that.bucket.createJsonItem(inputs.value, inputs.key).save(cb);
								} catch(err) {
									cb(err);
								}
							}
							, callback);
					}
				}
			});
		}
		, enumerable: true
	},

	createMany: {
		value: function createMany(kva) {
			dbc([kva && Array.isArray(kva)], 'kva (argument 0) must be an array');
			dbc([kva[0].value && typeof kva[0].value === 'object'], 'kv[0].value (argument 0) must be an object with a #value property');

			return new StreamingRepoOp(kva, this, 'create');
		}
	},

	update: {
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
					if(meta.validate && !meta.validate(kv.item))
					{
						callback(ResourceError.unprocessableEntity);
					} else {
						that.__write(kv
							, that.updatePipeline
							, function(inputs, cb) {
								try {
									that.bucket.createJsonItem(inputs.value, inputs.key).save(cb);
								} catch(err) {
									cb(err);
								}
							}
							, callback);
					}
				}
			});
		}
		, enumerable: true
	},

	updateMany: {
		value: function updateMany(kva) {
			dbc([kva && Array.isArray(kva)], 'kva (argument 0) must be an array');
			dbc([kva[0].value && typeof kva[0].value === 'object'], 'kv[0].value (argument 0) must be an object with a #value property');

			return new StreamingRepoOp(kva, this, 'update');
		}
	},

	delete: {
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
						, that.deletePipeline
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
	},

	deleteMany: {
		value: function deleteMany(kva, callback) {
			dbc([kva && Array.isArray(kva)], 'kva (argument 0) must be an array');
			dbc([kva[0] && (typeof kva[0].value === 'object' || typeof kva[0].key === 'string')], 'kva (argument 0) must have elements with #value (object) OR #key (string) properties');

			return new StreamingRepoOp(kva, this, 'delete');
		}
	},

	get: {
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
	},

	search: {
		value: function search(query, callback) {
			try {
				this.bucket.search.solr(query, callback);
			} catch(err) {
				callback(err);
			}
		}
		, enumerable: true
	},

	searchIndex: {
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
	},

	__write: {
		value: function __write(kv, pipeline, main, callback) {
			this.executePipeline(kv
				, pipeline.before
				, main
				, pipeline.on
				, pipeline.after
				, callback);
		}
	},

	bindPipeline: {
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
	},

	executePipeline: {
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