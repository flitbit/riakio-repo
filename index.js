var lib = require('./lib')
, pkg   = require('./package')
;

Object.defineProperties(module.exports, {
	version: { value: pkg.version, enumerable: true }
	, Repo: { value: lib, enumerable: true }
});