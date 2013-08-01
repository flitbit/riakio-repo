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

//{ id: '9079434148',  owner: '16257364@N00',  secret: '24a576e406',  server: '5501',  farm: 6,  title: '~',  ispublic: 1,  isfriend: 0,  isfamily: 0 }
r.get('16257364@N00_9079434148', function(err, res){
	if(err) {
		log.error(err.stack);
	} else {
		log.info(util.inspect(res, false, 99));
	}
});