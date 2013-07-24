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
	loadMetadataForBucket: {
		value: function loadMetadataForBucket(options) {
			var res
			, b
			, meta
			, refresh = false
			, bo = extend({}, options, { bucket: storeBucketName });
			, s = MetaBase.servers[options.server.name];
			if(s) {
				res = s[options.bucket];
				if(res) {
					return res;
				}
			}

			b = riakio.Bucket.create(bo);
			b.search.keys(riakio.KeyFilters.eq(options.bucket), function(err, res) {
				if(err) {
					if(err.httpEquivalent === 404) {
						meta = new metadata(options);
					} else {

					}
				} else {
					if(res.success) { 
						meta = res.result;
						refresh = true;
					} else if(res.httpEquivalent === 404) {
						meta = new metadata(options);
					}
				}
			});

			MetaBase.servers[options.server.name][options.bucket] = meta;
			if(refresh) {
				Timers.setTimeout(function(){
					MetaBase.servers[options.server.name][options.bucket] = null;
				}, 300000);
			}
			return meta;
		}
	}
	, clearMetadataForBucket: {
		value: function clearMetadataForBucket(options) {
			var s = MetaBase.servers[options.server.name];
			if(s) {
				s[options.bucket] = null;
			}
		}
	}
	, clearMetaBase: {
		value: function clearMetaBase() {
			MetaBase.servers = {};
		}
	}
});

module.exports = Metastore;