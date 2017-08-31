'use strict';
var assert = require('assert');
var util = require('util');

var RequestHandler = require('../../lib/request-handler');
var requests = require('../../lib/requests');
var helper = require('../test-helper');
var errors = require('../../lib/errors');
var types = require('../../lib/types');
var utils = require('../../lib/utils');
var retry = require('../../lib/policies/retry');
var ProfileManager = require('../../lib/execution-profile').ProfileManager;
var loadBalancing = require('../../lib/policies/load-balancing.js');
var reconnection = require('../../lib/policies/reconnection.js');

var options = (function () {
  return {
    policies: {
      loadBalancing: new loadBalancing.RoundRobinPolicy(),
      reconnection: new reconnection.ExponentialReconnectionPolicy(1000, 10 * 60 * 1000, false),
      retry: new retry.RetryPolicy()
    },
    socketOptions: {
      readTimeout: 0
    },
    logEmitter: helper.noop
  };
})();
describe('RequestHandler', function () {
  describe('#send()', function () {
    context('when an UNPREPARED response is obtained', function () {
      it('should send a prepare request on the same connection', function (done) {
        var queryId = utils.allocBufferFromString('123');
        var lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], undefined, function sendCallback(r, h, cb) {
          if (h.sendStreamCalled === 1) {
            // Its the first request, send an error
            var err = new errors.ResponseError(types.responseErrorCodes.unprepared, 'Test error');
            err.queryId = queryId;
            return cb(err);
          }
          cb(null, { });
        });
        var hosts = lbp.getFixedQueryPlan();
        var client = newClient(options, {
          getPreparedById: function (id) {
            return { query: 'QUERY1', id: id };
          }
        });
        var handler = newInstance(null, client, lbp);
        var request = new requests.ExecuteRequest('QUERY1', queryId, [], {});
        handler.send(request, {}, function (err, response) {
          assert.ifError(err);
          assert.ok(response);
          assert.strictEqual(hosts[0].prepareCalled, 1);
          assert.strictEqual(hosts[0].sendStreamCalled, 2);
          assert.strictEqual(hosts[1].prepareCalled, 0);
          assert.strictEqual(hosts[1].sendStreamCalled, 0);
          done();
        });
      });
      it('should move to next host when PREPARE response is an error', function (done) {
        var queryId = utils.allocBufferFromString('123');
        var lbp = helper.getLoadBalancingPolicyFake([ {}, {} ], function prepareCallback(q, h, cb) {
          if (h.address === '0') {
            return cb(new Error('Test error'));
          }
          cb();
        }, function sendFake(r, h, cb) {
          if (h.sendStreamCalled === 1) {
            // Its the first request, send an error
            var err = new errors.ResponseError(types.responseErrorCodes.unprepared, 'Test error');
            err.queryId = queryId;
            return cb(err);
          }
          cb(null, { });
        });
        var hosts = lbp.getFixedQueryPlan();
        var client = newClient(options, {
          getPreparedById: function (id) {
            return { query: 'QUERY1', id: id };
          }
        });
        var handler = newInstance(null, client, lbp);
        var request = new requests.ExecuteRequest('QUERY1', queryId, [], {});
        handler.send(request, {}, function (err, response) {
          assert.ifError(err);
          assert.ok(response);
          assert.strictEqual(hosts[0].prepareCalled, 1);
          assert.strictEqual(hosts[0].sendStreamCalled, 1);
          assert.strictEqual(hosts[1].prepareCalled, 1);
          assert.strictEqual(hosts[1].sendStreamCalled, 2);
          done();
        });
      });
    });
    it('should return a ResultSet with valid columns', function (done) {
      var handler = newInstance();
      var connection = { sendStream: function (r, o, cb) {
        setImmediate(function () {
          cb(null, {
            meta: {
              columns: [
                { type: { code: types.dataTypes.text, info: null}, name: 'col1'},
                { type: { code: types.dataTypes.list, info: { code: types.dataTypes.uuid, info: null}}, name: 'col2'}
              ],
              pageState: new Buffer('1234aa', 'hex')},
            flags: utils.emptyObject
          });
        });
      }};
      handler._getNextConnection = function (o, cb) {
        setImmediate(function () {
          handler.host = { setUp: helper.noop };
          cb(null, connection);
        });
      };
      //noinspection JSCheckFunctionSignatures
      handler.send(new requests.QueryRequest('Dummy QUERY'), {}, function (err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, types.ResultSet);
        assert.ok(util.isArray(result.columns));
        assert.strictEqual(result.columns.length, 2);
        assert.strictEqual(util.inspect(result.columns[0].type), util.inspect({code: types.dataTypes.text, info: null}));
        assert.strictEqual(result.columns[0].type.code, types.dataTypes.text);
        assert.strictEqual(result.columns[0].name, 'col1');
        assert.strictEqual(result.columns[1].type.code, types.dataTypes.list);
        assert.strictEqual(result.columns[1].type.info.code, types.dataTypes.uuid);
        assert.strictEqual(result.columns[1].name, 'col2');
        assert.ok(result.info);
        assert.strictEqual(result.pageState, '1234aa');
        done();
      });
    });
    it('should return a ResultSet with null columns when there is no metadata', function (done) {
      var handler = newInstance();
      var connection = { sendStream: function (r, o, cb) {
        setImmediate(function () {
          cb(null, { flags: utils.emptyObject });
        });
      }};
      handler._getNextConnection = function (o, cb) {
        setImmediate(function () {
          handler.host = { setUp: helper.noop };
          cb(null, connection);
        });
      };
      handler.send(new requests.QueryRequest('Dummy QUERY'), {}, function (err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, types.ResultSet);
        assert.strictEqual(result.columns, null);
        done();
      });
    });
    it('should use the retry policy defined in the constructor', function (done) {
      var policy = new retry.RetryPolicy();
      var policyCalled = 0;
      policy.onReadTimeout = function () {
        policyCalled++;
        return {decision: retry.RetryPolicy.retryDecision.retry};
      };
      var handler = newInstance(null, null, null, policy);
      var connectionCalled = 0;
      var connection = { sendStream: function (r, o, cb) {
        setImmediate(function () {
          if (connectionCalled++ < 2) {
            return cb(new errors.ResponseError(types.responseErrorCodes.readTimeout, 'dummy timeout'));
          }
          cb(null, { meta: {
            columns: [
              { type: { code: types.dataTypes.text, info: null}, name: 'col1'},
              { type: { code: types.dataTypes.list, info: { code: types.dataTypes.uuid, info: null}}, name: 'col2'}
            ],
            pageState: new Buffer('1234aa', 'hex')
          }});
        });
      }};
      handler._getNextConnection = function (o, cb) {
        setImmediate(function () {
          handler.host = { setUp: helper.noop };
          cb(null, connection);
        });
      };
      handler.send(new requests.QueryRequest('Dummy QUERY'), { }, function (err, result) {
        assert.ifError(err);
        helper.assertInstanceOf(result, types.ResultSet);
        //2 error responses, 2 retry decisions
        assert.strictEqual(policyCalled, 2);
        done();
      });
    });
    it('should callback with OperationTimedOutError when queryOptions.retryOnTimeout is set to false', function (done) {
      var handler = newInstance( { socketOptions: { readTimeout: 1234 }});
      handler.host = { address: '1.1.1.1:9042', checkHealth: helper.noop };
      var connection = { sendStream: function (r, o, cb) {
        cb(new errors.OperationTimedOutError('Testing timeout'));
      }};
      handler._getNextConnection = function (o, cb) {
        cb(null, connection);
      };
      var queryOptions = { retryOnTimeout: false};
      //noinspection JSCheckFunctionSignatures
      handler.send(new requests.QueryRequest('q'), queryOptions, function (err) {
        helper.assertInstanceOf(err, errors.OperationTimedOutError);
        assert.strictEqual(err.message, 'Testing timeout');
        done();
      });
    });
    it('should retry sending using the next host', function (done) {
      var handler = newInstance();
      var getNextConnectionCounter = 0;
      handler.host = { address: '1.1.1.1:9042', checkHealth: helper.noop, setUp: helper.noop };
      var connection1 = { sendStream: function (r, o, cb) {
        cb(new errors.OperationTimedOutError('Testing timeout'));
      }};
      var connection2 = { sendStream: function (r, o, cb) {
        cb(null, {});
      }};
      handler._getNextConnection = function (o, cb) {
        if (getNextConnectionCounter++ === 0) {
          return cb(null, connection1);
        }
        cb(null, connection2);
      };
      var queryOptions = { retryOnTimeout: true };
      //noinspection JSCheckFunctionSignatures
      handler.send(new requests.QueryRequest('q'), queryOptions, function (err) {
        assert.ifError(err);
        assert.strictEqual(getNextConnectionCounter, 2);
        done();
      });
    });

    var captureStackTraceOptions = [
      {options:{}, expected:false},
      {options:{captureStackTrace:true}, expected:true},
      {options:{captureStackTrace:false}, expected:false}
    ];

    captureStackTraceOptions.forEach(function(test) {
      var expect = test.expected ? '' : ' not';
      it('should return an error' + expect + ' including calling stack trace if captureStackTrace is ' + test.options.captureStackTrace, function (done) {
        var handler = newInstance();
        // Capture the current stack minus the top line in the call stack (since line number is not exact).
        var stack = {};
        Error.captureStackTrace(stack);
        stack = stack.stack.split('\n');
        stack.splice(0,2);
        stack = stack.join('\n');

        handler.host = { address: '1.1.1.1:9042', checkHealth: helper.noop, setUp: helper.noop };
        var connection1 = { sendStream: function (r, o, cb) {
          setImmediate(function () {
            cb(new errors.ResponseError(types.responseErrorCodes.syntaxError, 'syntax error'));
          });
        }};
        handler._getNextConnection = function (o, cb) {
          return cb(null, connection1);
        };
        //noinspection JSCheckFunctionSignatures
        handler.send(new requests.QueryRequest('q'), test.options, function (err) {
          helper.assertInstanceOf(err, errors.ResponseError);
          if(test.expected) {
            assert.ok(err.stack.indexOf('(event loop)') !== -1, err.stack + '\n\tdoes not contain (event loop)');
            assert.ok(err.stack.indexOf(stack) !== -1, err.stack + '\n\tdoes not contain\n' + stack);
          } else {
            assert.ok(err.stack.indexOf('(event loop)') === -1, err.stack + '\n\tcontains (event loop)');
            assert.ok(err.stack.indexOf(stack) === -1, err.stack + '\n\tcontains\n' + stack);
          }
          done();
        });
      });
    });
  });
});

/** @returns {RequestHandler} */
function newInstance(customOptions, client, loadBalancingPolicy, retryPolicy) {
  var o = utils.extend({}, options, customOptions);
  return new RequestHandler(
    client || newClient(o), loadBalancingPolicy || o.policies.loadBalancing, retryPolicy || o.policies.retry);
}

function newClient(o, metadata) {
  //noinspection JSCheckFunctionSignatures
  return {
    profileManager: new ProfileManager(o),
    options: o,
    metadata: metadata
  };
}