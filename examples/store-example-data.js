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

var d = data.slice(0, 50).map(function(el) { return { value: el }; })
;

r.createMany(d, function(err, res) {
	var op = res;

	op.on('data', function(err, res) {
		if(err) {
			log.error('EVENT: DATA: Error: '.concat(util.inspect(res, false, 99), '\n', err.stack));
		} else {
			log.info('EVENT: DATA: Response: '.concat(util.inspect(res, false, 99)));
		}
	});

	op.on('error', function(err, res) {
		if(err) {
			log.error('EVENT: ERROR: Error: '.concat(util.inspect(res, false, 99), '\n', err.stack));
		} else {
			log.info('EVENT: ERROR: Response: '.concat(util.inspect(res, false, 99)));
		}
	});

	op.on('canceled', function() {
		log.info('EVENT: CANCELED');
	});

	op.on('done', function() {
		log.info('EVENT: DONE');
	});

});