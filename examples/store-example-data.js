var util = require('util')
, winston = require('winston')
, data = require('../node_modules/riakio/examples/practice-data')
, repo = require('..')
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
, r = new repo({ bucket: 'flikr-photos', server: {name: 'dev' }, log: log })
;

data.forEach(function(ff) {
	r.create(ff, function(err, res){
		if(err) {
			log.error(util.inspect(err, true, 99));
		} else {
			log.info(util.inspect(res, true, 99));
		}
	});
});