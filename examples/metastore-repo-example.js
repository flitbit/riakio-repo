var util = require('util')
, winston = require('winston')
, repo = require('../lib/repo')
, meta = require('../lib/metadata')
, riakio = require('riakio')
;

riakio({servers: {
	dev: {
		scheme: 'http',
		host: 'riakdev.netsteps.local',
		port: 80
	}
}});

var log = new (winston.Logger)({
	transports: [new (winston.transports.Console)({ level: 'info' })]
})
, options = { bucket: '_bucket_metadata'
	, server: {name: 'dev' }
	, calculateKey: function(item) {
			return ''.concat(item.name);
		}
	, log: log }
, r = new repo(options)
;

var m = {createHooks: { before: [], on: [], after: [] }, name: 'flikr-photos'};

var Before1 = function(obj, next) {
	if(options.log && obj) {
		options.log.info('Before Create 1:'.concat(util.inspect(obj, false, 99)));
	}
	next(null, obj);
}
var Before2 = function(obj, next) {
	if(options.log && obj) {
		options.log.info('Before Create 2:'.concat(util.inspect(obj, false, 99)));
	}
	next(null, obj);
}
m.createHooks.before.push(Before1.toString());
m.createHooks.before.push(Before2.toString());


var On1 = function(obj) {
	if(options.log && obj) {
		options.log.info('On Create 1:'.concat(util.inspect(obj, false, 99)));
	}
}
var On2 = function(obj) {
	if(options.log && obj) {
		options.log.info('On Create 2:'.concat(util.inspect(obj, false, 99)));
	}
}
m.createHooks.on.push(On1.toString());
m.createHooks.on.push(On2.toString());


var After1 = function(obj, next) {
	if(options.log && obj) {
		options.log.info('After Create 1:'.concat(util.inspect(obj, false, 99)));
	}
	next(null, obj);
}
var After2 = function(obj, next) {
	if(options.log && obj) {
		options.log.info('After Create 2:'.concat(util.inspect(obj, false, 99)));
	}
	next(null, obj);
}
m.createHooks.after.push(After1.toString());
m.createHooks.after.push(After2.toString());

var me = new meta({}, m);

r.create({value: me}, function(err, res){
	if(err) {
		log.error(err.stack);
	} else {
		log.info(util.inspect(res, false, 99));
	}
});