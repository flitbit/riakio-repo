'use strict';

var riakio = require('riakio')
, webflow = require('webflow')
, Success = webflow.Success
, ResourceError = webflow.ResourceError
, util = require('util')
, extend = util._extend
, metadata = require('./metadata')
, url = require('url')
, storeBucketName = '_bucket_metadata'
, MetaBase = { buckets: { } }
;

Object.defineProperties(module.exports, {
	loadMetadataForBucket: {
		value: function loadMetadataForBucket(options, callback) {
			var b = riakio.Bucket.create(options)
			;
			b.items.fetch(options.forBucket, function(err, res) {
				var meta
				;
				if(err) {
					callback(err);
				} else if(res.success) {
					if(res.httpEquivalent === 404) {
						meta = new Success(new metadata(options), null, 404); 
					} else {
						meta = new Success(new metadata(options, res.result), null, res.httpEquivalent);
					}
					callback(null, meta);
				} else {
					//Recieved some unhandled resource response type... pass it back as error...
					callback(err || res);
				}
			});
		}
	}
	, getMetadataForBucket: {
		value: function getMetadataForBucket(options, bucket, callback) {
			var burl = url.format(bucket.baseUrl)
			;

			if(MetaBase.buckets[burl] && MetaBase.buckets[burl].meta) {
				callback(null, MetaBase.buckets[burl].meta);
			} else {
				if(MetaBase.buckets[burl] && MetaBase.buckets[burl].queue) {
					MetaBase.buckets[burl].queue.push(callback);
					if(options && options.log && options.log.info) {
						options.log.info('Repo Metastore load callback queued for bucket: '.concat(burl));
					}
				} else {
					if(!MetaBase.buckets[burl]) {
						MetaBase.buckets[burl] = { };
					}
					MetaBase.buckets[burl].queue = [callback];
					var bo = extend({}, options)
					;
					bo.forBucket = options.bucket;
					bo.bucket = storeBucketName;
					bo.calculateKey = function metastore_calculateKey(obj){ return obj.name; };
					this.loadMetadataForBucket(bo, function(err, res) {
						if(res) {
							MetaBase.buckets[burl].meta = res;
							if(!MetaBase.buckets[burl]._interval) {
								MetaBase.buckets[burl]._interval = setInterval(function(){
									try {
										loadMetadataForBucket(bo, function(err, res) {
											if(err) {
												if(bo && bo.log && bo.log.error) {
													try {
														bo.log.error(util.inspect(err, true, 99));
													} catch(e) { }
												}
											} else {
												MetaBase.buckets[burl].meta = res;
												if(bo && bo.log && bo.log.info) {
													bo.log.info('Repo Metastore reloaded Metadata for: '.concat(burl));
												}
											}
										});
									} catch (err) {
										if(bo && bo.log && bo.log.error) {
											try {
												bo.log.error(util.inspect(err, true, 99));
											} catch(e) { }
										}
									}
								}
								, bo.metaReloadMS || 300000);

								MetaBase.buckets[burl]._interval.unref();//Prevent keeping app running just for this...
							}
						}
						
						MetaBase.buckets[burl].queue.forEach(function (cb) {
							cb(err, MetaBase.buckets[burl].meta);
						});
						
						delete MetaBase.buckets[burl].queue;
					});
				}
			}
		}
		, enumerable: true
	}
	, clearMetadataForBucket: {
		value: function clearMetadataForBucket(bucket) {
			var b = (typeof bucket === 'object') 
				? url.format(bucket.baseUrl)
				: bucket
			;
			if(MetaBase.buckets[b]) {
				var iid = MetaBase.buckets[b]._interval;
				if(iid) {
					clearInterval(iid);
				}
				delete MetaBase.buckets[b]._interval;
				delete MetaBase.buckets[b]
			}
		}
		, enumerable: true
	}
	, clearMetaBase: {
		value: function clearMetaBase() {
			Object.keys(MetaBase.buckets).forEach(function(b) {
				this.clearMetadataForBucket(b);
			});
		}
		, enumerable: true
	}
});