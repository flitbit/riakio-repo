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

var d = data.slice(0, 500).map(function(el) { return { value: el }; })
;

r.deleteMany(d, function(err, res){
	if(err) {
		log.error(util.inspect(err, true, 99));
	} else {
		log.info(util.inspect(res, false, 99));
	}
});
