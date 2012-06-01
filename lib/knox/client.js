/**
 * TODO: HTTPS SUPPORT
 * TODO: Streamline
 */

/*!
 * knox - Client
 * Copyright(c) 2010 LearnBoost <dev@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var utils = require('./utils')
  , auth = require('./auth')
  , http = require('http')
  , url = require('url')
  , join = require('path').join
  , mime = require('./mime')
  , fs = require('fs')
  , crypto = require('crypto')
  , __ = require('underscore')
  , xml2js = require('xml2js');

/**
 * Initialize a `Client` with the given `options`.
 *
 * Required:
 *
 *  - `key`     amazon api key
 *  - `secret`  amazon secret
 *
 * @param {Object} options
 * @api public
 */

var Client = module.exports = exports = function Client(options) {
  this.endpoint = 's3.amazonaws.com';

  // TODO: HTTPS SUPPORT
  this.port = 80;

  if (!options.key) throw new Error('aws "key" required');
  if (!options.secret) throw new Error('aws "secret" required');
  if (options.endpoint) {
    this.endpoint = options.endpoint;
  }

  utils.merge(this, options);
};

Client.prototype.parseS3Path = function(key) {
  var slashIndex = key.indexOf('/');
  if (slashIndex < 0) {
    throw new Error('Invalid key, must have at least one slash to separate the bucket from the key');
  }
  return {bucket: key.substring(0, slashIndex), key: key.substring(slashIndex + 1)};
}

Client.prototype.buildRequestOptions = function(method, path, headers) {
  var options = {
    host: this.endpoint,
    port: this.port
  };

  var s3PathParts = this.parseS3Path(path);
  var key = s3PathParts.key;
  var bucket = s3PathParts.bucket;

  var date = new Date();
  var headers = headers || {};

  // Default headers
  utils.merge(headers, {
      Date: date.toUTCString()
    , Host: this.endpoint
  });

  options.path = encodeURI(join('/', bucket, key));

  // Authorization header
  headers.Authorization = auth.authorization({
      key: this.key
    , secret: this.secret
    , verb: method
    , date: date
    , resource: auth.canonicalizeResource(options.path)
    , contentType: headers['Content-Type']
    , md5: headers['Content-MD5'] || ''
    , amazonHeaders: auth.canonicalizeHeaders(headers)
  });

  // Issue request
  options.method = method;
  options.headers = headers;

  return options;
}

/**
 * Creates a new bucket
 */
Client.prototype.makeBucket = function(bucket, fn) {
  // Satisfy funky requirements
  if (bucket[bucket.length - 1] !== '/') {
    bucket += '/';
  }

  var request = this.request('PUT', encodeURI(bucket));

  request.on('response', function(res) {
    res.on('error', function(err) {
      fn(err, null);
    });

    res.on('end', function() {
      fn(null, res);
    });
  });

  request.end();
}

/**
 * Deletes a bucket. Note that the bucket must be empty before deleting it
 */
Client.prototype.deleteBucket = function(bucket, fn) {
  // Satisfy funky requirements
  if (bucket[bucket.length - 1] !== '/') {
    bucket += '/';
  }

  var request = this.request('DELETE', encodeURI(bucket));

  request.on('response', function(res) {
    res.on('error', function(err) {
      fn(err, null);
    });

    res.on('end', function() {
      fn(null, res);
    });
  });

  request.end();
}

// Make one request to list keys with the given prefix. Returns at most 1000
// results (S3 limit). Pass the last key you got as the optional 'marker'
// argument to scroll through more keys.
// Returns: { isTruncated: boolean, keys: [string] }
Client.prototype.reqKeysWithPrefix = function(path, marker, fn) {
  if (typeof marker === 'function') {
    fn = marker;
    marker = undefined;
  }

  var s3PathParts = this.parseS3Path(path);
  var prefix = s3PathParts.key;

  var uri = s3PathParts.bucket + '/?prefix=' + prefix;
  if (marker) {
    uri += "&marker=" + marker;
  }
  var request = this.request('GET', encodeURI(uri));

  request.on('error', function(e) {
    fn(e, null);
  });

  request.on('response', function(res) {
    var buffers = [];
    var totalBytes = 0;

    res.on('data', function(chunk) {
      buffers.push(chunk);
      totalBytes += chunk.length;
    });

    res.on('error', function(err) {
      fn(err, null);
    });

    res.on('end', function() {
      var data = new Buffer(totalBytes);
      var bytesWritten = 0;

      for (var i = 0; i < buffers.length; ++i) {
        buffers[i].copy(data, bytesWritten);
        bytesWritten += buffers[i].length;
      }

      new xml2js.Parser().parseString(data.toString(), function(err, data) {
        if (err) {
          fn(err, null);
        } else {
          var keys = [];
          var contents = data.Contents || [];

          // A single object is returned as an object, not an array
          if (!__.isArray(contents)) {
            contents = [contents];
          }

          for (var i = 0; i < contents.length; ++i) {
            keys.push(contents[i].Key);
          }

          var ans = {
            keys: keys,
            isTruncated: data.IsTruncated === "true"
          };

          fn(null, ans);
        }
      });
    });

  });

  request.end();

}

// Get all keys with the given prefix, making as many requests as necessary
// to get the complete list.
Client.prototype.getKeysWithPrefix = function(path, fn) {
  var keys = [];
  var self = this;

  var loop = function(marker) {
    return self.reqKeysWithPrefix(path, marker, function(err, ans) {
      if (err) {
        return fn(err);
      }

      // When the request includes a marker, S3 seems to begin the next page
      // of results with the key *following* marker. However, the REST API
      // documentation does not actually state whether marker is inclusive or
      // exclusive, so let's check anyway.
      if (ans.keys.length > 0 && ans.keys[0] === marker) {
        ans.keys.shift();
      }

      keys.push(ans.keys);

      if (ans.isTruncated) {        
        if (ans.keys.length < 1) {
          return fn(new Error('S3 claimed the ListBucketResult was truncated, yet returned no contents'));
        }

        var nextMarker = ans.keys[ans.keys.length-1];

        if (!nextMarker || nextMarker === marker) {
          return fn (new Error('S3 tried to send me into an infinite loop'));
        }

        return loop(nextMarker);
      }
      else {
        return fn(null, __.flatten(keys));
      }
    });
  };

  return loop(undefined);
}

/**
 * Request with `filename` the given `method`, and optional `headers`.
 *
 * @param {String} method
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api private
 */

Client.prototype.request = function(method, filename, headers){
  var req = http.request(this.buildRequestOptions(method, filename, headers));
  req.url = this.url(filename);

  return req;
};

/**
 * PUT data to `filename` with optional `headers`.
 *
 * Example:
 *
 *     // Fetch the size
 *     fs.stat('Readme.md', function(err, stat){
 *      // Create our request
 *      var req = client.put('/test/Readme.md', {
 *          'Content-Length': stat.size
 *        , 'Content-Type': 'text/plain'
 *      });
 *      fs.readFile('Readme.md', function(err, buf){
 *        // Output response
 *        req.on('response', function(res){
 *          console.log(res.statusCode);
 *          console.log(res.headers);
 *          res.on('data', function(chunk){
 *            console.log(chunk.toString());
 *          });
 *        });
 *        // Send the request with the file's Buffer obj
 *        req.end(buf);
 *      });
 *     });
 *
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api public
 */

Client.prototype.put = function(filename, headers){
  headers = utils.merge({
      Expect: '100-continue'
    , 'x-amz-acl': 'public-read'
  }, headers || {});
  return this.request('PUT', filename, headers);
};

/**
 * PUT the file at `src` to `filename`, with callback `fn`
 * receiving a possible exception, and the response object.
 *
 * NOTE: this method reads the _entire_ file into memory using
 * fs.readFile(), and is not recommended or large files.
 *
 * Example:
 *
 *    client
 *     .putFile('package.json', '/test/package.json', function(err, res){
 *       if (err) throw err;
 *       console.log(res.statusCode);
 *       console.log(res.headers);
 *     });
 *
 * @param {String} src
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.putFile = function(src, filename, headers, fn){
  var self = this;
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  };
  fs.readFile(src, function(err, buf){
    if (err) return fn(err);
    headers = utils.merge({
        'Content-Length': buf.length
      , 'Content-Type': mime.lookup(src)
      , 'Content-MD5': crypto.createHash('md5').update(buf).digest('base64')
    }, headers);
    self.put(filename, headers).on('response', function(res){
      res.on('end', function() {
        fn(null, res);
      });
    }).end(buf);
  });
};

/**
 * PUT the given `stream` as `filename` with optional `headers`.
 *
 * @param {Stream} stream
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.putStream = function(stream, filename, headers, fn){
  var self = this;
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  };

  var buffers = [];

  var chunkBufferer = function(chunk) {
    buffers.push(chunk);
  };

  var errorHandler = function(error) {
    return fn(err);
  };

  var endHandler = function() {
  };

  stream.on('data', chunkBufferer);
  stream.on('error', errorHandler);

  fs.stat(stream.path, function(err, stat){
    if (err) return fn(err);

    var req = self.put(filename, utils.merge({
        'Content-Length': stat.size
      , 'Content-Type': mime.lookup(stream.path)
    }, headers));

    req.on('response', function(res){
      res.on('end', function() {
        fn(null, res);
      });
    });

    // Clear out the previously buffered data
    for (var i = 0; i < buffers.length; ++i) {
      req.write(buffers[i]);
    }

    // Remove our earlier handlers
    stream.removeListener('data', chunkBufferer);
    stream.removeListener('error', errorHandler);

    if (stream.readable) {
      // Pipe the rest of the stream
      stream.pipe(req);
    } else {
      // Stream was already closed
      req.end();
    }
  });
};

/**
 * GET `filename` with optional `headers`.
 *
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api public
 */

Client.prototype.get = function(filename, headers){
  return this.request('GET', filename, headers);
};

/**
 * GET `filename` with optional `headers` and callback `fn`
 * with a possible exception and the response.
 *
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.getFile = function(filename, headers, fn){
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  }
  return this.get(filename, headers).on('response', function(res){
    res.on('end', function() {
      fn(null, res);
    });
  }).end();
};

/**
 * Issue a HEAD request on `filename` with optional `headers.
 *
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api public
 */

Client.prototype.head = function(filename, headers){
  return this.request('HEAD', filename, headers);
};

/**
 * Issue a HEAD request on `filename` with optional `headers`
 * and callback `fn` with a possible exception and the response.
 *
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.headFile = function(filename, headers, fn){
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  }
  return this.head(filename, headers).on('response', function(res){
    res.on('end', function() {
      fn(null, res);
    });
  }).end();
};

/**
 * DELETE `filename` with optional `headers.
 *
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api public
 */

Client.prototype.del = function(filename, headers){
  return this.request('DELETE', filename, headers);
};

/**
 * DELETE `filename` with optional `headers`
 * and callback `fn` with a possible exception and the response.
 *
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.deleteFile = function(filename, headers, fn){
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  }

  var request = this.del(filename, headers);

  request.on('error', function(err) {
    fn(err, null);
  });

  request.on('response', function(res){

    res.on('error', function(err) {
      fn(err, null);
    });

    res.on('end', function() {
      fn(null, res);
    });

  });

  request.end();
};

/**
 * Return a )url to the given `filename`.
 *
 * @param {String} path
 * @return {String}
 * @api public
 */

Client.prototype.url =
Client.prototype.http = function(path){
  var s3PathParts = this.parseS3Path(path);
  return encodeURI('http://' + join(this.endpoint, s3PathParts.bucket, s3PathParts.key));
};

/**
 * Return an HTTPS url to the given `filename`.
 *
 * @param {String} filename
 * @return {String}
 * @api public
 */

Client.prototype.https = function(filename){
  return 'https://' + join(this.endpoint, filename);
};

/**
 * Return an S3 presigned url to the given `filename`.
 *
 * @param {String} filename
 * @param {Date} expiration
 * @return {String}
 * @api public
 */

Client.prototype.signedUrl = function(filename, expiration){
  var epoch = Math.floor(expiration.getTime()/1000);
  var signature = auth.signQuery({
    secret: this.secret,
    date: epoch,
    resource: '/' + this.bucket + url.parse(filename).pathname
  });

  return this.url(filename) +
    '?Expires=' + epoch +
    '&AWSAccessKeyId=' + this.key +
    '&Signature=' + encodeURIComponent(signature);
};

/**
 * Shortcut for `new Client()`.
 *
 * @param {Object} options
 * @see Client()
 * @api public
 */

exports.createClient = function(options){
  return new Client(options);
};
