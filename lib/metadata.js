var util = require('util')
, extend = util._extend
, dbc = require('dbc.js')
, validate = require('jsonschema')
;

function Metadata (options, from) {
	var arg = extend({}, from)
	, _name = arg.name || options.forBucket
	, _schema = arg.schema || {}
	, _indices = arg.indices ? (Array.isArray(arg.indices) ? arg.indices : [arg.indices]) : []
	, _createHooks = arg.createHooks || { before: [], on: [], after: [] }
	, _createPipeline = { before: [], on: [], after: [] }
	, _updateHooks = arg.updateHooks || { before: [], on: [], after: [] }
	, _updatePipeline = { before: [], on: [], after: [] }
	, _deleteHooks = arg.deleteHooks || { before: [], on: [], after: [] }
	, _deletePipeline = { before: [], on: [], after: [] }
	, _validator = validate.Validator()
	;

	Object.defineProperties(this, {
		validator : { value: _validator }
		, name: { 
			get: function get_name () { return _name; }
			, enumerable: true
		}
		, schema: { 
			get: function get_schema() { return _schema; }
			, enumerable: true 
		}
		, indices: { 
			get: function get_indices() { return _indices; }
			, enumerable: true 
		}
		, createHooks: {
			get: function get_createHooks () { return _createHooks; }
			, enumerable: true
		}
		, createPipeline: {
			get: function get_createPipeline () { return _createPipeline; }
		}
		, updateHooks: {
			get: function get_updateHooks () { return _updateHooks; }
			, enumerable: true
		}
		, updatePipeline: {
			get: function get_updatePipeline () { return _updatePipeline; }
		}
		, deleteHooks: {
			get: function get_deleteHooks () { return _deleteHooks; }
			, enumerable: true
		}
		, deletePipeline: {
			get: function get_deletePipeline () { return _deletePipeline; }
		}
	});

	function evalHook (strFn) {
		eval('var fn = '.concat(strFn));
		return fn;
	}

	if(this.createHooks) {
		if(this.createHooks.before && this.createHooks.before.length) {
			this.createPipeline.before = this.createHooks.before.map(evalHook);
		}
		if(this.createHooks.on && this.createHooks.on.length) {
			this.createPipeline.on = this.createHooks.on.map(evalHook);
		}
		if(this.createHooks.after && this.createHooks.after.length) {
			this.createPipeline.after = this.createHooks.after.map(evalHook);
		}
	}
	if(this.updateHooks) {
		if(this.updateHooks.before && this.updateHooks.before.length) {
			this.updatePipeline.before = this.updateHooks.before.map(evalHook);
		}
		if(this.updateHooks.on && this.updateHooks.on.length) {
			this.updatePipeline.on = this.updateHooks.on.map(evalHook);
		}
		if(this.updateHooks.after && this.updateHooks.after.length) {
			this.updatePipeline.after = this.updateHooks.after.map(evalHook);
		}
	}
	if(this.deleteHooks) {
		if(this.deleteHooks.before && this.deleteHooks.before.length) {
			this.deletePipeline.before = this.deleteHooks.before.map(evalHook);
		}
		if(this.deleteHooks.on && this.deleteHooks.on.length) {
			this.deletePipeline.on = this.deleteHooks.on.map(evalHook);
		}
		if(this.deleteHooks.after && this.deleteHooks.after.length) {
			this.deletePipeline.after = this.deleteHooks.after.map(evalHook);
		}
	}
}

Object.defineProperties(Metadata.prototype, {
	validate : {
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