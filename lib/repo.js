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
	, _canceledEmitted = false
	, _doneEmitted = false
	, _todo = _items.length
	, _len = _items.length
	, _i = -1
	, self = this
	;

	Object.defineProperties(this, {

		canceled: {
			value: _canceledEmitted
			, enumerable: true
		},

		completed: {
			value: _doneEmitted
			, enumerable: true
		},

		canceledEmitted: {
			get: function get_canceledEmitted() { return _canceledEmitted; }
			, set: function set_canceledEmitted(val) { _canceledEmitted = val; }
		},

		doneEmitted: {
			get: function get_doneEmitted() { return _doneEmitted; }
			, set: function set_doneEmitted(val) { _doneEmitted = val; }
		}

	});

	function innerExec(item) {
		_repo[_op](item, function(err, res) {
			--_todo;
			if(_todo) {
				process.nextTick(function() {
					if(!_canceledEmitted) {
						self.emit('data', err, { item: item, result: res });
					}
				});
			} else { //emit final data and done in sync.
				if(!_canceledEmitted) {
					self.emit('data', err, { item: item, result: res });
				}
				self.doneEmitted = true;
				self.emit('done');
			}
		});
	}

	process.nextTick(function() {
		while (++_i < _len)
		{
			if(_canceledEmitted) {
				break;
			} else {
				innerExec(_items[_i]);
			}
		}
	});
}
util.inherits(StreamingRepoOp, EventEmitter);

Object.defineProperties(StreamingRepoOp.prototype, {

	cancel: {
		value: function cancel () {
			if(!this.completed && !this.canceledEmitted) {
				this.canceledEmitted = true;
				this.emit('canceled');
			} else if(this.completed) {
				this.emit('error', new Error('Operation already completed and cannot be Canceled!'));
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

		options: { value: _options },

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
				var self = this
				;
				metastore.getMetadataForBucket(this.options, this.bucket, function(err, res) {
					if(err) {
						callback(err);
					} else {
						self.meta = res;
						self.createPipeline = self.bindPipeline(self.meta.result.createPipeline);
						self.updatePipeline = self.bindPipeline(self.meta.result.updatePipeline);
						self.deletePipeline = self.bindPipeline(self.meta.result.deletePipeline);
						callback(null, self.meta);
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

			var self = this
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
						self.__write(kv
							, self.createPipeline
							, function(inputs, cb) {
								try {
									self.bucket.createJsonItem(inputs.value, inputs.key).save(cb);
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
		value: function createMany(kva, callback) {
			dbc([kva && Array.isArray(kva)], 'kva (argument 0) must be an array');
			dbc([kva[0].value && typeof kva[0].value === 'object'], 'kv[0].value (argument 0) must be an object with a #value property');

			var op = new StreamingRepoOp(kva, this, 'create');
			if(callback) {
				callback(null, op);
			} else {
				return op;
			}
		}
	},

	update: {
		value: function update(kv, callback) {
			dbc([kv && typeof kv === 'object'], 'kv (argument 0) must be an object');
			dbc([kv.value && typeof kv.value === 'object'], 'kv.value (argument 0) must be an object with a #value property');
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			var self = this
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
						self.__write(kv
							, self.updatePipeline
							, function(inputs, cb) {
								try {
									self.bucket.createJsonItem(inputs.value, inputs.key).save(cb);
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
		value: function updateMany(kva, callback) {
			dbc([kva && Array.isArray(kva)], 'kva (argument 0) must be an array');
			dbc([kva[0].value && typeof kva[0].value === 'object'], 'kv[0].value (argument 0) must be an object with a #value property');

			var op = new StreamingRepoOp(kva, this, 'update');
			if(callback) {
				callback(null, op);
			} else {
				return op;
			}
		}
	},

	del: {
		value: function del(kv, callback) {
			dbc([kv && typeof kv === 'object'], 'kv (argument 0) must be an object');
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			var self = this
			;
			this.metadata(function(err, res) {
				if(err) {
					callback(err);
				} else {
					var meta = res.result
					;

					self.__write(kv
						, self.deletePipeline
						, function(inputs, cb) {
							try {
								var k = (inputs.key) ? (inputs.key || self.bucket.calculateKey(inputs.value)) : self.bucket.calculateKey(inputs.value);
								self.bucket.items.remove(k, cb);
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

	delMany: {
		value: function delMany(kva, callback) {
			dbc([kva && Array.isArray(kva)], 'kva (argument 0) must be an array');
			dbc([kva[0] && (typeof kva[0].value === 'object' || typeof kva[0].key === 'string')], 'kva (argument 0) must have elements with #value (object) OR #key (string) properties');

			var op = new StreamingRepoOp(kva, this, 'del');
			if(callback) {
				callback(null, op);
			} else {
				return op;
			}
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

	getMany: {
		value: function getMany(keyArr, callback) {
			dbc([keyArr, Array.isArray(keyArr), keyArr.length], 'keyArr (argument 0) is required, must be any array, and must have elements')
			dbc([typeof callback === 'function'], 'callback (argument 1) must be a function');

			var op = new StreamingRepoOp(keyArr, this, 'get');
			if(callback) {
				callback(null, op);
			} else {
				return op;
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
			, self = this
			;

			if(funcs.before && funcs.before.length) {
				result.before = funcs.before.map(function(fn) { return fn.bind(self); });
			}
			if(funcs.on && funcs.on.length) {
				result.on = funcs.on.map(function(fn) { return fn.bind(self); });
			}
			if(funcs.after && funcs.after.length) {
				result.after = funcs.after.map(function(fn) { return fn.bind(self); });
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
				var self = this
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
										self.emit('error', e);
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