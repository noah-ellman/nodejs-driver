'use strict';
var helper = require('../test-helper');
var http = require('http');
var spawn = require('child_process').spawn;
var util = require('util');
var fs = require('fs');
var utils = require('../../lib/utils.js');
var Client = require('../../lib/client.js');

var simulacronHelper = {
  _execute: function(processName, params, cb) {
    var originalProcessName = processName;

    // If process hasn't completed in 10 seconds.
    var timeout = undefined;
    if(cb) {
      timeout = setTimeout(function() {
        cb('Timed out while waiting for ' + processName + ' to complete.');
      }, 10000);
    }

    var p = spawn(processName, params, {});
    p.stdout.setEncoding('utf8');
    p.stderr.setEncoding('utf8');
    p.stdout.on('data', function (data) {
      helper.trace('%s_out> %s', originalProcessName, data);
    });

    p.stderr.on('data', function (data) {
      helper.trace('%s_err> %s', originalProcessName, data);
    });

    p.on('close', function (code) {
      helper.trace('%s exited with code %d', originalProcessName, code);
      if(cb) {
        clearTimeout(timeout);
        if (code === 0) {
          cb();
        } else {
          cb(Error('Process exited with non-zero exit code: ' + code));
        }
      }
    });

    return p;
  },
  start: function(cb) {
    var self = this;
    var simulacronJarPath = process.env['SIMULACRON_PATH'];
    if (!simulacronJarPath) {
      simulacronJarPath = process.env['HOME'] + "/simulacron.jar";
      helper.trace("SIMULACRON_PATH not set, using " + simulacronJarPath);
    }
    if (!fs.existsSync(simulacronJarPath)) {
      throw new Error('Simulacron jar not found at: ' + simulacronJarPath);
    }

    var processName = 'java';
    var params = ['-jar', simulacronJarPath, '--ip', '127.0.0.101'];
    var initialized = false;

    var timeout = setTimeout(function() {
      cb(new Error('Timed out while waiting for Simulacron server to start.'));
    }, 10000);

    self.sProcess = self._execute(processName, params, function() {
      if(!initialized) {
        cb();
      }
    });
    self.sProcess.stdout.on('data', function (data) {
      // This is a bit of a kludge, check for a particular log statement which indicates
      // that all principals have been created before invoking the completion callback.
      if(data.indexOf('Started HTTP server interface') !== -1) {
        clearTimeout(timeout);
        helper.trace('Simulacron initialized!');
        initialized = true;
        cb();
      }
    });
  },
  stop: function(cb) {
    if(this.sProcess !== undefined) {
      if(this.sProcess.exitCode) {
        helper.trace('Server already stopped with exit code %d.', this.sProcess.exitCode);
        cb();
      } else {
        this.sProcess.on('close', function () {
          cb();
        });
        this.sProcess.on('error', cb);
        if (process.platform.indexOf('win') === 0) {
          var params = ['Stop-Process', this.sProcess.pid];
          this._execute('powershell', params, cb);
        } else {
          this.sProcess.kill('SIGINT');
        }

      }
    } else {
      cb(Error('Process is not defined.'));
    }
  },
  setup: function (dcs, options) {
    var self = this;
    options = options || utils.emptyObject;
    var clientOptions = options.clientOptions || {};
    var simulacronCluster = new SimulacronCluster();
    var initClient = options.initClient !== false;
    var client;
    before(function (done) {
      self.start(function () {
        simulacronCluster.start(dcs, clientOptions, function() {
          done();
        });
      });
    });
    if (initClient) {
      var baseOptions = { contactPoints: ['127.0.0.101'] };
      client = new Client(utils.extend({}, options.clientOptions, baseOptions));
      before(client.connect.bind(client));
      after(client.shutdown.bind(client));
    }
    afterEach(simulacronCluster.clear.bind(simulacronCluster));
    after(simulacronHelper.stop.bind(simulacronHelper));

    var setupInfo = { cluster: simulacronCluster, client: client };
    return setupInfo;
  },
  baseOptions: (function () {
    return {
      //required
      cassandraVersion: helper.getCassandraVersion(),
      dseVersion: '',
      clusterName: 'testCluster',
      activityLog: true,
      numTokens: 1
    };
  })(),
  baseAddress: 'localhost',
  defaultPort: 8187,
  SimulacronCluster: SimulacronCluster
};

function makeRequest(options, callback) {
  var request = http.request(options, function(response) {
    // Continuously update stream with data
    var body = '';
    response.on('data', function(d) {
      body += d;
    });
    response.on('end', function() {
      if (body === '') {
        callback(null);
      } else {
        callback(JSON.parse(body));
      }
    });
  });
  request.on('error', function(err) {
    helper.trace(err.message);
    throw new Error(err);
  });
  return request;
}

function SimulacronTopic() {
  this.baseAddress = simulacronHelper.baseAddress;
  this.port = simulacronHelper.defaultPort; 
}

SimulacronTopic.prototype._getPath = function (endpoint, id) {
  var path = '/' + endpoint + '/' + id;
  return encodeURI(path);
};

SimulacronTopic.prototype._getOptions = function (endpoint, id, method) {
  return {
    host: this.baseAddress,
    path: this._getPath(endpoint, id),
    port: this.port,
    method: method,
    headers: { 'Content-Type': 'application/json' }
  };
};

SimulacronTopic.prototype.getLogs = function(callback) {
  var self = this;
  makeRequest(this._getOptions('log', this.id, 'GET'), function(data) {
    callback(null, self._filterLogs(data));
  }).end();
};

SimulacronTopic.prototype.clearLogs = function(callback) {
  makeRequest(this._getOptions('log', this.id, 'DELETE'), function(data) {
    callback(null, data);
  }).end();
};

SimulacronTopic.prototype.primeQueryWithEmptyResult = function(queryStr, callback) {
  this.prime({
    when: {
      query: queryStr
    },
    then: {
      result: 'success',
      delay_in_ms: 0,
      rows: [],
      column_types: {}
    }
  }, callback);
};

SimulacronTopic.prototype.prime = function(body, callback) {
  var request = makeRequest(this._getOptions('prime', this.id, 'POST'), function(data) {
    callback(null, data);
  });
  request.write(JSON.stringify(body));
  request.end();
};

SimulacronTopic.prototype.clearPrimes = function(callback) {
  makeRequest(this._getOptions('prime', this.id, 'DELETE'), function(data) {
    callback(null, data);
  }).end();
};

SimulacronTopic.prototype.clear = function(callback) {
  var self = this;
  utils.parallel([
    self.clearPrimes.bind(self), 
    self.clearLogs.bind(self)
  ], callback);
};

SimulacronTopic.prototype.stop = function(callback) {
  var stopNodePath = '/listener/%s?type=stop';
  var options = {
    host: this.baseAddress,
    path: encodeURI(util.format(stopNodePath, this.id)),
    port: this.port,
    method: 'DELETE'
  };
  makeRequest(options, function(data) {
    callback(data);
  }).end();
};

SimulacronTopic.prototype.resume = function(callback) {
  var resumeNodePath = '/listener/%s';
  var options = {
    host: this.baseAddress,
    path: encodeURI(util.format(resumeNodePath, this.id)),
    port: this.port,
    method: 'PUT'
  };
  makeRequest(options, function(data) {
    callback(data);
  }).end();
};

SimulacronTopic.prototype._filterLogs = function(data) {
  // TODO implement for cluster and dc.
  return data;
};

function SimulacronCluster() {
  SimulacronTopic.call(this);
}

util.inherits(SimulacronCluster, SimulacronTopic);

function SimulacronDataCenter(cluster, dc) {
  SimulacronTopic.call(this);
  this.cluster = cluster;
  this.data = dc;
  this.localId = dc.id;
  this.id = cluster.id + '/' + dc.id;
}

util.inherits(SimulacronDataCenter, SimulacronTopic);

function SimulacronNode(dc, node) {
  SimulacronTopic.call(this);
  this.dc = dc;
  this.data = node;
  this.localId = node.id;
  this.id = dc.id + '/' + node.id;
}

util.inherits(SimulacronNode, SimulacronTopic);

SimulacronTopic.prototype._filterLogs = function(data) {
  return data.data_centers[0].nodes[0].queries;
};

SimulacronCluster.prototype.start = function(dcs, clientOptions, callback) {
  var self = this;
  var createClusterPath = '/cluster?data_centers=%s&cassandra_version=%s&dse_version=%s&name=%s&activity_log=%s&num_tokens=%d';

  var options = utils.extend({}, simulacronHelper.baseOptions, clientOptions);

  var urlPath = encodeURI(util.format(createClusterPath, dcs, options.cassandraVersion, options.dseVersion,
    options.clusterName, options.activityLog, options.numTokens));

  var requestOptions = {
    host: self.baseAddress,
    port: self.port,
    path: urlPath,
    method: 'POST'
  };

  makeRequest(requestOptions, function(data) {
    self.name = data.name;
    self.id = data.id;
    self.dcs = data.data_centers;
    callback(null);
  }).end();
};

SimulacronCluster.prototype.destroy = function(callback) {
  makeRequest(this._getOptions('cluster', this.id, 'DELETE'), function(data) {
    callback();
  }).end();
};

// TODO implement cluster.dc and dc.node

SimulacronCluster.prototype.node = function() {
  var args = arguments;
  var dc;
  var node;
  if (typeof args[0] === "string") {
    // assume by address - TODO replace impl.
    var data = this.findNode(args[0]);
    dc = new SimulacronDataCenter(this, this.dcs[data.dataCenterId]);
    node = new SimulacronNode(dc, dc.data.nodes[data.nodeId]);
  } else {
    var dcId = 0;
    var nodeId;
    if (args.length === 1) {
      nodeId = args[0];
    } else {
      dcId = args[0];
      nodeId = args[1];
    }
    dc = new SimulacronDataCenter(this, this.dcs[dcId]);
    node = new SimulacronNode(dc, dc.data.nodes[nodeId]);
  }
  return node;
};

SimulacronCluster.prototype.findNode = function(nodeAddress) {
  var self = this;

  function findInDc(dc) {
    for (var nodeId = 0; nodeId < dc.nodes.length; nodeId++) {
      if (dc.nodes[nodeId].address === nodeAddress) {
        return {
          nodeId: dc.nodes[nodeId].id,
          dataCenterId: dc.id
        };
      }
    }
  }
 
  for(var dcIndex = 0; dcIndex < self.dcs.length; dcIndex++) {
    var nodeFound = findInDc(self.dcs[dcIndex]);
    if (nodeFound) {
      return nodeFound;
    }
  }
};

SimulacronCluster.prototype.getContactPoints = function(dataCenterId) {
  var dcId = dataCenterId = typeof dataCenterId === 'undefined' ? 0 : dataCenterId;
  return this.dcs[dcId].nodes.map(function (node) {
    return node.address;
  });
};

module.exports = simulacronHelper;
