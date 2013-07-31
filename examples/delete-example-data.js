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

var d = data.slice(0, 500).map(function(el) { return { key: ''.concat(el.owner, '_', el.id) }; })
;

var rop = r.deleteMany(d);
rop.on('data', function(err, res) {
	if(err) {
		log.error('Data event recieved Error: '.concat(util.inspect(res, false, 99), '\n', err.stack));
	} else {
		log.info('Data event recieved Response: '.concat(util.inspect(res, false, 99)));
	}
});
rop.on('error', function(err, res) {
	if(err) {
		log.error('Error event recieved Error: '.concat(util.inspect(res, false, 99), '\n', err.stack));
	} else {
		log.info('Error event recieved Response: '.concat(util.inspect(res, false, 99)));
	}
});
rop.on('done', function() {
	log.info('!!!!!!!!!!!!!! DONE !!!!!!!!!!!!!!');
});

rop.exec();
