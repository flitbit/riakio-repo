var util = require('util')
, winston = require('winston')
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

//{ "id": "9032192043", "owner": "10734133@N02", "secret": "e17ea45db8", "server": "7312", "farm": 8, "title": "the blue basket", "ispublic": 1, "isfriend": 0, "isfamily": 0},
r.get('10734133@N02_9032192043', function(err, res){
	if(err) {
		log.error(err.stack);
	} else {
		log.info(util.inspect(res, false, 99));
	}
});