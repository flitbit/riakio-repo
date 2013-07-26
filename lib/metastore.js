'use strict';

var riakio = require('riakio')
, webflow = require('webflow')
, Success = webflow.Success
, ResourceError = webflow.ResourceError
, util = require('util')
, extend = util._extend
, metadata = require('./metadata')
, storeBucketName = '_bucket_metadata'
, MetaBase = { servers: { } }
;

Object.defineProperties(module.exports, {
	loadMetadataForBucket: {
		value: function loadMetadataForBucket(options, callback) {
			var b = riakio.Bucket.create(options)
			;
			b.items.fetch(options.bucket, function(err, res) {
				var meta
				;
				if(err) {
					if(err.httpEquivalent === 404) {
						meta = new Success(new metadata(options), null, 200);
					} else {
						callback(err);
					}
				} else {
					if(res.success) { 
						meta = new Success(new metadata(options, res.result), null, res.httpEquivalent);
					} else {
						meta = new Success(new metadata(options), null, 200); //Not sure how we got here...
					}

					var s = options.server.name
					, b = options.bucket
					;

					if(!MetaBase.servers[s]) {
						MetaBase.servers[s]= { };
					}
					if(!MetaBase.servers[s][b]){
						MetaBase.servers[s][b] = { };
						MetaBase.servers[s][b].meta = meta;
						if(!MetaBase.servers[s][b]._interval) {
							MetaBase.servers[s][b]._interval = setInterval(function(){
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
						}
					}					
					callback(null, meta);
				}
			});
		}
	}
	, getMetadataForBucket: {
		value: function getMetadataForBucket(options, callback) {
			var res
			, b = options.bucket
			, s = MetaBase.servers[options.server.name]
			;
			if(s) {
				res = s[b];
				if(res) {
					callback(null, res.meta);
				}
			} else {
				var bo = extend({}, options)
				;
				bo.bucket = storeBucketName;
				bo.calculateKey = function metastore_calculateKey(obj){ return obj.name; };
				this.loadMetadataForBucket(bo, callback);
			}
		}
		, enumerable: true
	}
	, clearMetadataForBucket: {
		value: function clearMetadataForBucket(options) {
			var s = MetaBase.servers[options.server.name]
			, b = options.bucket
			;
			if(s && s[b]) {
				var iid = s[b]._interval;
				if(iid) {
					clearInterval(iid);
				}
				delete s[b]._interval;
				delete s[b];
			}
		}
		, enumerable: true
	}
	, clearMetaBase: {
		value: function clearMetaBase() {
			Object.keys(MetaBase.servers).forEach(function(s) {
				Object.keys(MetaBase.servers[s]).forEach(function(b){
					this.clearMetadataForBucket({ bucket: b, server: { name: s } });
				});
			});
		}
		, enumerable: true
	}
});