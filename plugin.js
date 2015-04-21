
const AWS = require("aws-sdk");
const S3 = require("s3");
const URL = require("url");
const CRYPTO = require("crypto");


exports.for = function (API) {

	var exports = {};

	exports.resolve = function (resolver, config, previousResolvedConfig) {

		return resolver({}).then(function (resolvedConfig) {

			function distribution (distributionId) {

				var distribution = resolvedConfig.distributions[distributionId];

				return API.Q.nbind(API.getFileTreeHashFor, API)(distribution.source).then(function (hash) {

					var bucketName = distribution.bucket;

					distribution.url = "http://" + API.PATH.join(URL.parse(S3.getPublicUrlHttp(bucketName, "")).hostname, (distribution.path || ""));

					distribution.sourceHash = hash;
				});
			}

			return API.Q.all(Object.keys(resolvedConfig.distributions).map(distribution)).then(function () {
				return resolvedConfig;
			});
		});
	}

	exports.turn = function (resolvedConfig) {

		var s3low = new AWS.S3(new AWS.Config({
			accessKeyId: resolvedConfig.accessKeyId,
			secretAccessKey: resolvedConfig.secretAccessKey,
			apiVersion: '2006-03-01'
		}));

		var s3high = S3.createClient({
		  	s3Client: s3low
		});

		function distribution (distributionId) {

			API.console.debug("Check distribution '" + distributionId + "'");

			var distribution = resolvedConfig.distributions[distributionId];

			return API.Q.denodeify(function (callback) {

				var bucketName = distribution.bucket;
				var uploadedPaths = [];

				API.console.debug("Ensure uploaded to S3 bucket '" + bucketName + "'");

				function checkIfChanged (callback) {

					var key = API.PATH.join(distribution.path || "", ".pinf.config.hash");

					API.console.debug("Check if changed:", key);
	
			        return s3low.getObject({
			            Bucket: bucketName,
			            Key: key
			        }, function (err, response) {
			            if (err) {
			                if (err.statusCode === 404) {
			                	// Nothing found. So we say its changed.
			                    return callback(null, true);
			                }
			                API.console.debug("err.statusCode", err.statusCode, "path", API.PATH.join(distribution.path || "", ".pinf.config.hash"));
			                API.console.debug("err.message", err.message);
			                return callback(err);
			            }
			            var remoteHash = response.Body.toString();
						API.console.debug("remoteHash:", remoteHash);
			            return API.FS.readFile(API.PATH.join(distribution.source, ".pinf.config.hash"), "utf8", function (err, localHash) {
			            	if (err) return callback(err);
			            	if (remoteHash === localHash) {
			            		// Nothing has changed.
			            		return callback(null, false);
			            	}
			            	// Something has changed.
		            		return callback(null, true);
			            });
			        });
				}

				// TODO: Download manifest to see what has changed.
				//       If no manifest, list files to see if changed

				function ensureBucket (_callback) {

					function callback (err) {
						if (err) return _callback(err);
						return _callback(null, URL.parse(S3.getPublicUrlHttp(bucketName, "")).hostname);
					}

					return s3low.listBuckets({}, function (err, _buckets) {
						if (err) return callback(err);

						var found = false;
						if (_buckets.Buckets) {
							_buckets.Buckets.forEach(function (bucket) {
								if (found) return; // TODO: exit foreach
								if (bucket.Name === bucketName) {
									found = true;
								}
							});
						}

						function ensureAccessPolicy (callback) {
							API.console.debug("Ensuring public access policy for bucket '" + bucketName + "'")
							return s3low.getBucketPolicy({
								Bucket: bucketName
							}, function(err, data) {
								if (err) {
									if (err.statusCode === 404) {
										return s3low.putBucketPolicy({
											Bucket: bucketName,
											// @see https://ariejan.net/2010/12/24/public-readable-amazon-s3-bucket-policy/
											Policy: JSON.stringify({
											  "Version":"2008-10-17",
											  "Statement":[
											    {
											      "Sid": "AllowPublicRead",
											      "Effect": "Allow",
											      "Principal": {
											        "AWS": "*"
											      },
											      "Action": [
											        "s3:GetObject"
											      ],
											      "Resource":[
											        "arn:aws:s3:::" + bucketName + "/*"
											      ]
											    }
											  ]
											})
										}, function(err, data) {
											if (err) return callback(err);

											return callback(null);
										});
									}
									return callback(err);
								}
								return callback();
							});									
						}

						if (found) {
							return ensureAccessPolicy(callback);
						}

						API.console.verbose("Creating bucket '" + bucketName + "'")

						function create (callback) {
							return s3low.createBucket({
								Bucket: bucketName,
								ACL: "public-read"
							}, callback);
						}

						return create(function (err) {
							if (err) return callback(err);

							return ensureAccessPolicy(callback);
						});
					});
				}

				function upload (callback) {
					var uploader = s3high.uploadDir({
						localDir: distribution.source,
						deleteRemoved: false,
						s3Params: {
							Bucket: bucketName,
							Prefix: distribution.path || "",
							ACL: 'public-read'
							// See: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#putObject-property 
						},
					});
			        uploader.on('fileUploadStart', function (fullPath, fullKey) {
			        	uploadedPaths.push(fullKey);
			        });

					uploader.on("error", callback);
					uploader.on("progress", function () {
						API.console.debug("progress", uploader.progressAmount, uploader.progressTotal);
					});
					return uploader.on('end', callback);
				}

				return checkIfChanged(function (err, changed) {
					if (err) return callback(err);
					if (changed === false) {
						API.console.debug("Skipping upload. No change detected.");
						return callback(null);
					}
					return ensureBucket(function (err, s3BucketHost) {
						if (err) return callback(err);

						return upload(function (err) {
							if (err) return callback(err);

							API.console.debug("Uploaded paths:", uploadedPaths);

							return callback(null);
						});
					});
				});
			})();
		}

		return API.Q.all(Object.keys(resolvedConfig.distributions).map(distribution));
	}

	exports.spin = function (resolvedConfig) {
	}

	return exports;
}
