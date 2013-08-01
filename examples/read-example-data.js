var util = require('util')
, winston = require('winston')
, repo = require('../lib/repo')
, riakio = require('riakio')
, data = require('../node_modules/riakio/examples/practice-data')
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

var d = data.slice(26, 36).map(function(el) { return ''.concat(el.owner, '_', el.id); })
;

// { "id": "7038965665", "owner": "97042891@N00", "secret": "11048fbc5d", "server": "7264", "farm": 8, "title": "Vista Chinesa RJ - Pão de Açúcar", "ispublic": 1, "isfriend": 0, "isfamily": 0},
// { "id": "3841805569", "owner": "97042891@N00", "secret": "b902b7a3a0", "server": "2623", "farm": 3, "title": "Parque da Cidade - Niterói - Morro da Viração - Rio de Janeiro - Brasil -  Pão de Açúcar - Corcovado", "ispublic": 1, "isfriend": 0, "isfamily": 0},
// { "id": "2300837831", "owner": "97042891@N00", "secret": "a1bc05c959", "server": "2317", "farm": 3, "title": "Rio de Janeiro - Brasil - Rio - Brazil Rio 2016 - Cristo Redentor - Carnaval - samba - futebol - praia - carnival - football - beach", "ispublic": 1, "isfriend": 0, "isfamily": 0},
// { "id": "7881804566", "owner": "97042891@N00", "secret": "a817458766", "server": "8286", "farm": 9, "title": "Baia de Guanabara fotografada do Parque das Ruínas", "ispublic": 1, "isfriend": 0, "isfamily": 0},
// { "id": "8068824212", "owner": "97042891@N00", "secret": "520822b9e4", "server": "8459", "farm": 9, "title": "Crepúsculo dos Deuses - Twilight - Corcovado", "ispublic": 1, "isfriend": 0, "isfamily": 0},
// { "id": "9067335291", "owner": "89311841@N04", "secret": "f468dfdac0", "server": "7417", "farm": 8, "title": "Sizzle", "ispublic": 1, "isfriend": 0, "isfamily": 0},
// { "id": "2836360729", "owner": "8398907@N02", "secret": "6500249fe6", "server": "3005", "farm": 4, "title": "California Sea Monster, Beware, Dangerous When Fed Cookies", "ispublic": 1, "isfriend": 0, "isfamily": 0},
// { "id": "9066505445", "owner": "37414352@N02", "secret": "64d929d087", "server": "7338", "farm": 8, "title": "Spring reflection with trees  [Explored]", "ispublic": 1, "isfriend": 0, "isfamily": 0},
// { "id": "9066505955", "owner": "18481658@N00", "secret": "113aa0e954", "server": "2879", "farm": 3, "title": "No darkness.", "ispublic": 1, "isfriend": 0, "isfamily": 0},
// { "id": "9059262373", "owner": "63583522@N00", "secret": "2c86a6deee", "server": "3701", "farm": 4, "title": "The sound of the sea helps me get back to me.", "ispublic": 1, "isfriend": 0, "isfamily": 0},
// { "id": "9061831696", "owner": "16047105@N06", "secret": "48e60f29a3", "server": "7293", "farm": 8, "title": "Kiss", "ispublic": 1, "isfriend": 0, "isfamily": 0},
r.getMany(d, function(err, res) {
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