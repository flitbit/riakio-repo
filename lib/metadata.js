"use strict";

var util = require('util')
, extend = util._extend
, dbc = require('dbc.js')
;

function Metadata (repo, from) {
	var arg = extend({}, from)
	, _schema = arg.schema || {}
	, _indices = arg.indices ? (Array.isArray(arg.indices) ? arg.indices : [arg.indices]) : []
	, _preCommitHooks = arg.preCommitHooks ? (Array.isArray(arg.preCommitHooks) ? arg.preCommitHooks : [arg.preCommitHooks]) : [] 
	, _postCommitHooks = arg.postCommitHooks ? (Array.isArray(arg.postCommitHooks) ? arg.postCommitHooks : [arg.postCommitHooks]) : [] 
	;

	Object.defineProperties(this, {
		repo: { value: repo }
		, schema: { 
			get: function() { return _schema; }
			, set: function(val) {
				//TODO validate 'val' conforms to schema format...
				_schema = val;
			}
			, enumerable: true }
		, indices { get: function() { return _indices; }, enumerable: true }
		, preCommitHooks: { get: function() { return _preCommitHooks; }, enumerable: true }
		, postCommitHooks: { get: function() { return _postCommitHooks; }, enumerable: true }
	});
}

Object.defineProperties(Metadata.prototype, {
	addIndex: {
		value: function addIndex(ptr) {
			this.indices.push(ptr);
		}
		, enumerable: true
	}
	, addPreCommitHook: {
		value: function addPreCommitHook(fn) {
			dbc([typeof fn !== 'function'], 'Pre-Commit Hooks must be functions.')

			this.preCommitHooks.push(fn);
		}
	}
	, addPostCommitHook: {
		value: function addPostCommitHook(fn) {
			dbc([typeof fn !== 'function'], 'Post-Commit Hooks must be functions.')

			this.postCommitHooks.push(fn);
		}
	}
	, save: {
		value: function save() {
			repo.update(function(err, res) {
					if(err) {
						console.log(util.inspect(err, true, 99));
					} else {

					}
				}
				, this
				, '_metadata'
			});
		}
	}

	}
});

module.exports = Metadata;