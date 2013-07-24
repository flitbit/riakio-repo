'use strict';

var riakio = require('riakio')
, util = require('util')
, extend = util._extend
, metadata = require('./metadata')
, storeBucketName = '_bucket_metadata'
, MetaBase = { servers: { } }
;

function Metastore () {
}

Object.defineProperties(Metastore, {
	loadMetaDataForBucket: {
		value: function loadMetadataForBucket(options, callback) {
			b = riakio.Bucket.create(options);
			b.search.keys(riakio.KeyFilters.eq(options.bucket), function(err, res) {
				var meta
				;
				if(err) {
					if(err.httpEquivalent === 404) {
						meta = new metadata(options);
					} else {
						callback(err);
					}
				} else {
					if(res.success) { 
						meta = res.result;
						refresh = true;
					} else if(res.httpEquivalent === 404) {
						meta = new metadata(options);
					}
				}

				MetaBase.servers[options.server.name][options.bucket] = meta;

				Timers.setTimeout(function(){
					try {
						loadMetadataForBucket(options, function(err, res) { 
							if(options && options.log) {
								if(err && options.log.error) {
									try {
										options.log.error(util.inspect(err, true, 99));
									} catch(e) { }
								} else if(res && options.log.info) {
									options.log.info('Repo Metastore reloaded Metadata for `'.concat(options.bucket, '`.'));
								}
							}
						});
					} catch (err) {
						if(options && options.log && options.log.error) {
							try {
								options.log.error(util.inspect(err, true, 99));
							} catch(e) { }
						}
					}
				}
				, options.metaReloadMS || 300000);

				callback(null, meta);
			});
		}
	}
	, getMetadataForBucket: {
		value: function getMetadataForBucket(options, callback) {
			var res
			, b
			, s = MetaBase.servers[options.server.name]
			;
			if(s) {
				res = s[options.bucket];
				if(res) {
					callback(null, res);
				}
			} else {
				var bo = extend({}, options, { bucket: storeBucketName, calculateKey: function(obj){ return obj.name; } })
				;
				loadMetadataForBucket(bo, callback);
			}
		}
		, enumerable: true
	}
	, clearMetadataForBucket: {
		value: function clearMetadataForBucket(options) {
			var s = MetaBase.servers[options.server.name];
			if(s) {
				s[options.bucket] = null;
			}
		}
		, enumerable: true
	}
	, clearMetaBase: {
		value: function clearMetaBase() {
			MetaBase.servers = {};
		}
		, enumerable: true
	}
});

module.exports = Metastore;