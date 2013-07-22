"use strict";

var util = require('util')
, extend = util._extend
, dbc = require('dbc.js')
, validate = require('jsonschema')
;

function Metadata (repo, from) {
	var arg = extend({}, from)
	, _schema = arg.schema || {}
	, _indices = arg.indices ? (Array.isArray(arg.indices) ? arg.indices : [arg.indices]) : []
	, _preCommitHooks = arg.preCommitHooks || {}
	, _postCommitHooks = arg.postCommitHooks || {}
	, _validator = validate.Validator()
	;

	Object.defineProperties(this, {
		repo: { value: repo }
		, validator : { value: _validator }
		, schema: { 
			get: function() { return _schema; }
			, set: function(val) {
				//TODO validate 'val' conforms to schema format...
				_schema = val;
			}
			, enumerable: true 
		}
		, indices { get: function() { return _indices; }, enumerable: true }
		, preCommitHooks: { get: function() { return _preCommitHooks; }, enumerable: true }
		, postCommitHooks: { get: function() { return _postCommitHooks; }, enumerable: true }
	});

	if(this.preCommitHooks) {
		var prek = Object.keys(this.preCommitHooks);
		if(prek && prek.length) {
			prek.forEach(function(el){
				this.bindPreCommitHook(eval(this.preCommitHooks[el]));
			});
		}
	}

	if(this.postCommitHooks) {
		var postk = Object.keys(this.postCommitHooks);
		if(postk && postk.length) {
			postk.forEach(function(el){
				this.bindPostCommitHook(eval(this.postCommitHooks[el]));
			});
		}
	}
}

Object.defineProperties(Metadata.prototype, {
	addIndex: {
		value: function addIndex(ptr) {
			this.indices.push(ptr);
		}
		, enumerable: true
	}
	, addPreCommitHook: {
		value: function addPreCommitHook(name, fn) {
			dbc([typeof name === 'string'], 'name (argument 0) must be a string');
			dbc([typeof fn === 'function'], 'fn (argument 1) must be a function');

			this.preCommitHooks[name] = fn.toString();
			this.bindPreCommitHook(fn);
		}
		, enumerable: true
	}
	, bindPreCommitHook: {
		value: function bindPreCommitHook(fn) {
			this.repo.before('create', fn);
			this.repo.before('update', fn);
			this.repo.before('delete', fn);
		}
	}
	, addPostCommitHook: {
		value: function addPostCommitHook(name, fn) {
			dbc([typeof name === 'string'], 'name (argument 0) must be a string');
			dbc([typeof fn === 'function'], 'fn (argument 1) must be a function');

			this.postCommitHooks[name] = fn.toString();
			this.bindPostCommitHook(fn);
		}
		, enumerable: true
	}
	, bindPostCommitHook: {
		value: function bindPostCommitHook(fn) {
			this.repo.after('create', fn);
			this.repo.after('update', fn);
			this.repo.after('delete', fn);
		}
	}
	, save: {
		value: function save(callback) {
			dbc([typeof callback === 'function'], 'callback (arguement 0) should be a function');
			try {
				repo.update(function(err, res) {
					if(err) {
						callback(err);
					} else {
						callback(null, res.result);
					}
				}
				, this
				, '_metadata');
			} catch(err) {
				callback(err);
			}
		}
		, enumerable: true
	}
	, validate : {
		value: function validate (obj) {
			if(this.schema) {
				var v = this.validator.validate(obj, this.schema);
				return !v.errors || v.errors.length == 0;
			} else {
				return true;
			}
		}
		, enumerable: true
	}
});

module.exports = Metadata;