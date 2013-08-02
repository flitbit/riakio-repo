var util = require('util')
, winston = require('winston')
, data = require('../node_modules/riakio/examples/practice-data')
, repo = require('../lib/repo')
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
, r = new repo({ bucket: 'flikr-photos'
	, server: {name: 'dev' }
	, calculateKey: function(item) {
			return ''.concat(item.owner, '_', item.id);
		}
	, log: log })
;

var d = data.slice(0, 50).map(function(el) { return { key: ''.concat(el.owner, '_', el.id) }; })
;

r.delMany(d, function(err, res) {
	var op = res;

	op.on('data', function(err, res) {
		if(err) {
			log.error('EVENT: DATA: Error: '.concat(util.inspect(err, false, 99), '\nStack: ', err.stack));
		} else {
			log.info('EVENT: DATA: Response: '.concat(util.inspect(res, false, 99)));
		}
	});

	op.on('error', function(err, res) {
		if(err) {
			log.error('EVENT: ERROR: Error: '.concat(util.inspect(err, false, 99), '\nStack: ', err.stack));
		} else {
			log.info('EVENT: ERROR: Response: '.concat(util.inspect(res, false, 99)));
		}
	});

	op.on('canceled', function(err, res) {
		if(err) {
			log.error('EVENT: CANCELED: Error: '.concat(util.inspect(err, false, 99), '\nStack: ', err.stack));
		} else {
			log.info('EVENT: CANCELED: Response: '.concat(util.inspect(res, false, 99)));
		}
	});

	op.on('done', function(err, res) {
		if(err) {
			log.error('EVENT: DONE: Error: '.concat(util.inspect(err, false, 99), '\nStack: ', err.stack));
		} else {
			log.info('EVENT: DONE: Response: '.concat(util.inspect(res, false, 99)));
		}
	});

});