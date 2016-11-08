var assert = require('assert-plus');
var bunyan = require('bunyan');
var jsprim = require('jsprim');
var moray = require('moray');
var vasync = require('vasync');
var libuuid = require('libuuid');

var morayTools = require('./lib/moray');

var CONFIG = require('./config.json')
var morayConfig = jsprim.deepCopy(CONFIG);
morayConfig.log = bunyan.createLogger({
    name: 'moray-client'
});

var BENCHMARK_BUCKET_NAME = 'moray_benchmark_unindexed';
var BENCHMARK_BUCKET_CFG_V0 = {
    index: {
        uuid: {
            type: 'string',
        }
    },
    options: {
        version: 0
    }
};
var BENCHMARK_BUCKET_CFG_V1 = {
    index: {
        uuid: {
            type: 'string',
        },
        not_yet_reindexed_boolean: {
            type: 'boolean'
        },
        not_yet_reindexed_string: {
            type: 'string'
        },
        not_yet_reindexed_number: {
            type: 'number'
        }
    },
    options: {
        version: 1
    }
};
var NB_TOTAL_OBJECTS = 10000;

var NB_OBJECTS_SENTINEL = NB_TOTAL_OBJECTS / 2;
assert.ok(NB_OBJECTS_SENTINEL > 0, 'NB_OBJECTS_SENTINEL must be > 0');

var NB_OBJECTS_NON_SENTINEL = NB_TOTAL_OBJECTS / 2;
assert.ok(NB_OBJECTS_NON_SENTINEL > 0, 'NB_OBJECTS_NON_SENTINEL must be > 0');

function getSentinelValueForType(typeName) {
    assert.string(typeName, 'typeName');

    switch (typeName) {
        case 'string':
            return 'sentinel';
        case 'boolean':
            return true;
        case 'number':
            return 42;
        default:
            assert(false, 'unsupported type: ' + typeName);
    }
}

function getNonSentinelValueForType(typeName) {
    assert.string(typeName, 'typeName');

    switch (typeName) {
        case 'string':
            return 'nonSentinel';
        case 'boolean':
            return false;
        case 'number':
            return 24;
        default:
            assert(false, 'unsupported type: ' + typeName);
    }
}

var morayClient = moray.createClient(morayConfig);
morayClient.on('connect', function onMorayConnected() {
    var context = {};

    vasync.pipeline({funcs: [
        function getBenchmarkBucket(ctx, next) {
            morayClient.getBucket(BENCHMARK_BUCKET_NAME,
                function onGetBucket(getBucketErr, bucket) {
                    ctx.bucket = bucket;

                    if (!getBucketErr ||
                        getBucketErr.name === 'BucketNotFoundError') {
                        next();
                    } else {
                        next(getBucketErr);
                    }
                });
        },
        function createBenchmarkBucket(ctx, next) {
            assert.optionalObject(ctx.bucket, 'ctx.bucket');

            if (ctx.bucket) {
                next();
                return;
            }

            console.log('Creating bucket %s...', BENCHMARK_BUCKET_NAME);

            morayClient.createBucket(BENCHMARK_BUCKET_NAME,
                BENCHMARK_BUCKET_CFG_V0, next);
        },
        function updateBenchmarkBucket(ctx, next) {
            assert.optionalObject(ctx.bucket, 'ctx.bucket');

            if (ctx.bucket) {
                next();
                return;
            }

            morayClient.updateBucket(BENCHMARK_BUCKET_NAME,
                BENCHMARK_BUCKET_CFG_V1, next);
        },
        function addSentinelObjects(ctx, next) {
            if (ctx.bucket) {
                next();
                return;
            }

            console.log('Adding %d sentinel objects...', NB_OBJECTS_SENTINEL);

            morayTools.addObjects(morayClient, BENCHMARK_BUCKET_NAME, {
                not_yet_reindexed_string: getSentinelValueForType('string'),
                not_yet_reindexed_number: getSentinelValueForType('number'),
                not_yet_reindexed_boolean: getSentinelValueForType('boolean')
            }, NB_OBJECTS_SENTINEL, next);
        },
        function addNonSentinelObjects(ctx, next) {
            if (ctx.bucket) {
                next();
                return;
            }

            console.log('Adding %d non-sentinel objects...',
                NB_OBJECTS_NON_SENTINEL);

            morayTools.addObjects(morayClient, BENCHMARK_BUCKET_NAME, {
                not_yet_reindexed_string: getNonSentinelValueForType('string'),
                not_yet_reindexed_number: getNonSentinelValueForType('number'),
                not_yet_reindexed_boolean: getNonSentinelValueForType('boolean')
            }, NB_OBJECTS_NON_SENTINEL, next);
        },
        function searchOnUnindexedString(ctx, next) {
            var filter = '(&(uuid=*)(not_yet_reindexed_string=' +
                getSentinelValueForType('string')  +  '))';

            morayTools.searchForObjects(morayClient, BENCHMARK_BUCKET_NAME,
                filter, {
                    requiredBucketVersion: 1
                }, {
                    nbObjectsExpected: NB_OBJECTS_SENTINEL,
                    expectedProperties: [
                        {
                            name: 'not_yet_reindexed_string',
                            value: 'foo'
                        }
                    ]
                }, next);
        },
        function searchOnUnindexedBoolean(ctx, next) {
            var filter = '(&(uuid=*)(not_yet_reindexed_boolean=' +
                getSentinelValueForType('boolean') + '))'
            morayTools.searchForObjects(morayClient, BENCHMARK_BUCKET_NAME,
                filter, {
                    requiredBucketVersion: 1
                }, {
                    nbObjectsExpected: NB_OBJECTS_SENTINEL,
                    expectedProperties: [
                        {
                            name: 'not_yet_reindexed_boolean',
                            value: true
                        }
                    ]
                }, next);
        },
        function searchOnUnindexedNumber(ctx, next) {
            var filter = '(&(uuid=*)(not_yet_reindexed_number=' +
                getSentinelValueForType('number') + '))';
            morayTools.searchForObjects(morayClient, BENCHMARK_BUCKET_NAME,
                filter, {
                    requiredBucketVersion: 1
                }, {
                    nbObjectsExpected: NB_OBJECTS_SENTINEL,
                    expectedProperties: [
                        {
                            name: 'not_yet_reindexed_number',
                            value: 42
                        }
                    ]
                }, next);
        }
    ], arg: context}, function allDone(err) {
        morayClient.close();

        if (err) {
            console.log('Error:', err);
        } else {
            console.log('All done!')
        }
    });
});