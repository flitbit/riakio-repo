var lib = require('./lib')
, pkg   = require('./package')
;

Object.defineProperty(lib, 'version', { value: pkg.version, enumerable: true });

module.exports = lib;