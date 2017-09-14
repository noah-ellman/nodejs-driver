'use strict';
var assert = require('assert');
var responseErrorCodes = require('../../../lib/types').responseErrorCodes;
var simulacron = require('../simulacron');
var utils = require('../../../lib/utils');

var Client = require('../../../lib/client.js');
var ConstantSpeculativeExecutionPolicy = require('../../../lib/policies/speculative-execution').ConstantSpeculativeExecutionPolicy;
var OrderedLoadBalancingPolicy = require('../../test-helper').OrderedLoadBalancingPolicy;

describe('Client', function() {
  describe('With ConstantSpeculativeExecutionPolicy', function () {
    var delay = 100;
    var clientOptions = {
      policies: { 
        speculativeExecution: new ConstantSpeculativeExecutionPolicy(delay, 3), 
        loadBalancing: new OrderedLoadBalancingPolicy()
      }
    };
    var setupInfo = simulacron.setup('3', { clientOptions: clientOptions });
    var client = setupInfo.client;
    var cluster = setupInfo.cluster;
    var query = "select * from data";
    
    var assertQueryCount = queryCounter(query, cluster);

    it('should not start speculative executions if query is non-idempotent', function (done) {
      utils.series([
        function primeNode0(next) {
          // prime node 0 to respond at 3x delay.
          cluster.prime('0/0', {
            when: {
              query: query
            },
            then : {
              result: "success",
              delay_in_ms: delay * 3
            }
          }, next);
        },
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: false }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 0, "Should not have been any speculative executions");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 1);
            
            // Should have queried 0th node.
            var queriedHost = cluster.findNode(result.info.queriedHost);
            assert.strictEqual(queriedHost.nodeId, 0);
           
            // Should have only sent request to node 0.
            assertQueryCount(0, 1);
            assertQueryCount(1, 0);
            assertQueryCount(2, 0);
            next();
          });
        }
      ], done);
    });
    it ('should complete from first speculative execution when faster', function (done) {
      utils.series([
        function primeNode0(next) {
          // prime node 0 to respond at 3x delay.
          cluster.prime('0/0', {
            when: {
              query: query
            },
            then : {
              result: "success",
              delay_in_ms: delay * 3
            }
          }, next);
        },
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 1, "Should have been one speculative execution");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 2);

            // Should have got response from 1st node.
            var queriedHost = cluster.findNode(result.info.queriedHost);
            assert.strictEqual(queriedHost.nodeId, 1);

            // Should have sent requests to node 0 and 1.
            assertQueryCount(0, 1);
            assertQueryCount(1, 1);
            assertQueryCount(2, 0);
            next();
          });
        }
      ], done);
    });
    it ('should complete from initial execution when speculative is started but is slower', function (done) {
      utils.series([
        function prime(next) {
          // prime all nodes at 4x delay, this should cause 2 speculative executions.
          cluster.prime({
            when: {
              query: query
            },
            then : {
              result: "success",
              delay_in_ms: delay * 4 
            }
          }, next);
        },
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 2, "Should have been two speculative executions");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 3);

            // Should have got response from 0th node, the initial query.
            var queriedHost = cluster.findNode(result.info.queriedHost);
            assert.strictEqual(queriedHost.nodeId, 0);

            // Should have sent requests to all nodes.
            assertQueryCount(0, 1);
            assertQueryCount(1, 1);
            assertQueryCount(2, 1);
            next();
          });
        }
      ], done);
    });
    it ('should complete from second speculative execution when faster', function (done) {
      utils.series([
        function primeNode0(next) {
          // prime node 0 to respond at 3x delay.
          cluster.prime('0/0', {
            when: {
              query: query
            },
            then : {
              result: "success",
              delay_in_ms: delay * 3 
            }
          }, next);
        },
        function primeNode1(next) {
          // prime node 1 to respond at 3x delay.
          cluster.prime('0/1', {
            when: {
              query: query
            },
            then : {
              result: "success",
              delay_in_ms: delay * 3
            }
          }, next);
        },
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 2, "Should have been two speculative executions");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 3);

            // Should have got response from 2nd node, the second speculative execution.
            var queriedHost = cluster.findNode(result.info.queriedHost);
            assert.strictEqual(queriedHost.nodeId, 2);

            // Should have sent requests to all nodes.
            assertQueryCount(0, 1);
            assertQueryCount(1, 1);
            assertQueryCount(2, 1);
            next();
          });
        }
      ], done);
    });
    it ('should retry within initial execution', function (done) {
      utils.series([
        function primeNode0(next) {
          // prime node 0 to respond with bootstrapping error (triggers retry on next node)
          cluster.prime('0/0', {
            when: {
              query: query
            },
            then : {
              result: "is_bootstrapping"
            }
          }, next);
        },
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 0, "Should have been no speculative executions");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 2);

            // Should have got response from 1st node, the retry from the first error.
            var queriedHost = cluster.findNode(result.info.queriedHost);
            assert.strictEqual(queriedHost.nodeId, 1);

            // Expect isBootstrapping error on host that failed.
            var failureHost = cluster.dcs[0].nodes[0].address;
            var code = result.info.triedHosts[failureHost].code;
            assert.strictEqual(code, responseErrorCodes.isBootstrapping, "Expected isBootstrapping");

            // Should have sent requests to two nodes.
            assertQueryCount(0, 1);
            assertQueryCount(1, 1);
            assertQueryCount(2, 0);
            next();
          });
        }
      ], done);
    });
    it ('should retry within speculative execution', function (done) {
      utils.series([
        function primeNode0(next) {
          // prime node 0 to respond at 3x delay.
          cluster.prime('0/0', {
            when: {
              query: query
            },
            then : {
              result: "success",
              delay_in_ms: delay * 3 
            }
          }, next);
        },
        function primeNode1(next) {
          // prime node 1 to respond with bootstrapping error (triggers retry on next node)
          cluster.prime('0/1', {
            when: {
              query: query
            },
            then : {
              result: "is_bootstrapping"
            }
          }, next);
        },
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 1, "Should have been one speculative execution");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 3);

            // Should have got response from 2nd node, the retry after the speculative execution.
            var queriedHost = cluster.findNode(result.info.queriedHost);
            assert.strictEqual(queriedHost.nodeId, 2);

            // Expect isBootstrapping error on host that failed.
            var failureHost = cluster.dcs[0].nodes[1].address;
            var code = result.info.triedHosts[failureHost].code;
            assert.strictEqual(code, responseErrorCodes.isBootstrapping, "Expected isBootstrapping");

            // Should have sent requests to all nodes.
            assertQueryCount(0, 1);
            assertQueryCount(1, 1);
            assertQueryCount(2, 1);
            next();
          });
        }
      ], done);
    });
    it ('should wait for last execution to complete', function (done) {
      utils.series([
        function primeNode0(next) {
          // prime node 0 to respond at 3x delay.
          cluster.prime('0/0', {
            when: {
              query: query
            },
            then : {
              result: "success",
              delay_in_ms: delay * 3 
            }
          }, next);
        },
        function primeNode1(next) {
          // prime node 1 to respond with bootstrapping error (triggers retry on next node)
          cluster.prime('0/1', {
            when: {
              query: query
            },
            then : {
              result: "is_bootstrapping"
            }
          }, next);
        },
        function primeNode2(next) {
          // prime node 2 to respond with bootstrapping error (no more hosts to retry, so should wait for node 0)
          cluster.prime('0/2', {
            when: {
              query: query
            },
            then : {
              result: "is_bootstrapping"
            }
          }, next);
        },
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 1, "Should have been one speculative execution");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 3);

            // Should have got response from 0th node.
            var queriedHost = cluster.findNode(result.info.queriedHost);
            assert.strictEqual(queriedHost.nodeId, 0);

            // Expect isBootstrapping error on both hosts that failed.
            [1, 2].forEach(function(id) {
              var failureHost = cluster.dcs[0].nodes[id].address;
              var code = result.info.triedHosts[failureHost].code;
              assert.strictEqual(code, responseErrorCodes.isBootstrapping, "Expected isBootstrapping");
            });

            // Should have sent requests to all nodes.
            assertQueryCount(0, 1);
            assertQueryCount(1, 1);
            assertQueryCount(2, 1);
            next();
          });
        }
      ], done);
    });
    it ('should fail if all executions fail and reach end of query plan', function (done) {
      utils.series([
        function primeNodes(next) {
          // prime each node with a shrinking delay
          // node 0: 3*delay
          // node 1: 2*delay
          // node 2: 1*delay
          utils.times(3, function(id, nextT) {
            cluster.prime('0/' + id, {
              when: {
                query: query
              },
              then : {
                result: "is_bootstrapping",
                delay_in_ms: (3 - id) * delay
              }
            }, nextT);
          }, next);
        },
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ok(err);
            assert.strictEqual(Object.keys(err.innerErrors).length, 3);

            // Expect isBootstrapping error on both hosts that failed.
            [0, 1, 2].forEach(function(id) {
              var failureHost = cluster.dcs[0].nodes[id].address;
              var code = err.innerErrors[failureHost].code;
              assert.strictEqual(code, responseErrorCodes.isBootstrapping, "Expected isBootstrapping");
            });

            // Should have sent requests to all nodes.
            assertQueryCount(0, 1);
            assertQueryCount(1, 1);
            assertQueryCount(2, 1);
            next();
          });
        }
      ], done);
    });
    it('should allow zero delay', function (done) {
      var clientOptions = {
        contactPoints: cluster.getContactPoints(0),
        policies: { 
          speculativeExecution: new ConstantSpeculativeExecutionPolicy(0, 3),
        }
      };
      var client = new Client(clientOptions);

      utils.series([
        client.connect.bind(client),
        function primeNode0(next) {
          // prime node 0 to respond at 4x delay.
          cluster.prime('0/0', {
            when: {
              query: query
            },
            then : {
              result: "success",
              delay_in_ms: delay * 4 
            }
          }, next);
        },
        function primeNode1(next) {
          // prime node 1 to respond at 4x delay.
          cluster.prime('0/1', {
            when: {
              query: query
            },
            then : {
              result: "success",
              delay_in_ms: delay * 4
            }
          }, next);
        },
        function primeNode2(next) {
          // prime node 2 to respond at 2x delay.
          cluster.prime('0/2', {
            when: {
              query: query
            },
            then : {
              result: "success",
              delay_in_ms: delay * 2
            }
          }, next);
        },
        function executeQuery(next) {
          client.execute(query, [], { isIdempotent: true }, function (err, result) {
            assert.ifError(err);
            assert.strictEqual(result.info.speculativeExecutions, 2, "Should have been two speculative executions");
            assert.strictEqual(Object.keys(result.info.triedHosts).length, 3);

            // Should have got response from 2nd node, since it had the lowest delay.
            var queriedHost = cluster.findNode(result.info.queriedHost);
            assert.strictEqual(queriedHost.nodeId, 2);

            // Should have sent requests to all nodes.
            assertQueryCount(0, 1);
            assertQueryCount(1, 1);
            assertQueryCount(2, 1);
            next();
          });
        },
        client.shutdown.bind(client)
      ], done);
    });
  });
});

// TODO generalize this and move to simulacron.
// Generates a function used for getting the number of times a query was received for a particular node.
function queryCounter(query, cluster) {
  return function (dcId, nodeId, expected) {
    if (!expected) {
      expected = nodeId;
      nodeId = dcId;
      dcId = 0;
    }
    var path = dcId + '/' + nodeId;
    cluster.log(path, function(err, result) {
      assert.ifError(err);
      var queries = result.data_centers[dcId].nodes[0].queries;
      var matches = queries.filter(function (el) {
        return el.query === query;
      });
      assert.strictEqual(matches.length, expected, "For node " + path);
    });
  };
}