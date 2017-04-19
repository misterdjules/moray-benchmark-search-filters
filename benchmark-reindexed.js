var assert = require('assert-plus');
var bunyan = require('bunyan');
var dashdash = require('dashdash');
var jsprim = require('jsprim');
var moray = require('moray');
var vasync = require('vasync');
var libuuid = require('libuuid');
var VError = require('verror').VError;

var mockedData = require('./lib/mocked-data');
var morayTools = require('./lib/moray');

var CONFIG = require('./config.json')
var morayConfig = jsprim.deepCopy(CONFIG);
morayConfig.log = bunyan.createLogger({
    name: 'moray-client'
});

var cmdLineOptions = [
    {
        names: ['help', 'h'],
        type: 'bool',
        help: 'Print this help and exit.'
    },
    {
        names: ['nbobjects', 'n'],
        type: 'number',
        help: 'Number of objects to create/find'
    },
    {
        names: ['findobjectsopts', 'opts'],
        type: 'string',
        help: 'additional options to pass to findobjects'
    }
];

var BENCHMARK_BUCKET_NAME = 'moray_benchmark_reindexed';
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
        reindexed_boolean: {
            type: 'boolean'
        },
        reindexed_string: {
            type: 'string'
        },
        reindexed_number: {
            type: 'number'
        }
    },
    options: {
        version: 1
    }
};

var DEFAULT_NB_TOTAL_OBJECTS = 1000;

var parser = dashdash.createParser({options: cmdLineOptions});
var parsedCmdLineOpts;
try {
    parsedCmdLineOpts = parser.parse(process.argv);
} catch (e) {
    console.error('error when parsing command line: %s', e.message);
}

if (parsedCmdLineOpts) {
    main(parsedCmdLineOpts);
}

function main(parsedCmdLineOpts) {
    var help;

    if (parsedCmdLineOpts.help) {
        help = parser.help({includeEnv: true}).trimRight();
        console.log('usage: node benchmark-unindexed [OPTIONS]\n'
                    + 'options:\n'
                    + help);
        return;
    }

    var NB_TOTAL_OBJECTS = parsedCmdLineOpts.nbobjects;
    if (NB_TOTAL_OBJECTS === undefined) {
        NB_TOTAL_OBJECTS = DEFAULT_NB_TOTAL_OBJECTS;
    }

    var NB_OBJECTS_SENTINEL = Math.max(Math.floor(NB_TOTAL_OBJECTS / 2), 1);
    assert.ok(NB_OBJECTS_SENTINEL > 0, 'NB_OBJECTS_SENTINEL must be > 0');

    var NB_OBJECTS_NON_SENTINEL = NB_TOTAL_OBJECTS - NB_OBJECTS_SENTINEL
    assert.ok(NB_OBJECTS_NON_SENTINEL >= 0,
        'NB_OBJECTS_NON_SENTINEL must be >= 0');

    assert.equal(NB_TOTAL_OBJECTS, NB_OBJECTS_SENTINEL +
        NB_OBJECTS_NON_SENTINEL);

    if (parsedCmdLineOpts.findobjectsopts === undefined) {
        parsedCmdLineOpts.findobjectsopts = {};
    }

    var findobjectsOpts = {};

    if (typeof (parsedCmdLineOpts.findobjectsopts) === 'string' &&
        parsedCmdLineOpts.findobjectsopts !== '') {
        findobjectsOpts = JSON.parse(parsedCmdLineOpts.findobjectsopts);
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
                            VError.findCauseByName(getBucketErr,
                                'BucketNotFoundError')) {
                            next();
                        } else {
                            next(getBucketErr);
                        }
                    });
            },
            function deleteBenchmarkBucket(ctx, next) {
                assert.optionalObject(ctx.bucket, 'ctx.bucket');

                if (ctx.bucket === undefined) {
                    next();
                    return;
                }

                console.log('Deleting bucket %s...', BENCHMARK_BUCKET_NAME);

                morayClient.deleteBucket(BENCHMARK_BUCKET_NAME,
                    function onDelBucket(delBucketErr) {
                        if (delBucketErr === undefined ||
                            delBucketErr === null) {
                            ctx.bucket = undefined;
                        }

                        next(delBucketErr);
                    });
            },
            function createBenchmarkBucket(ctx, next) {
                assert.equal(ctx.bucket, undefined);

                console.log('Creating bucket %s...', BENCHMARK_BUCKET_NAME);

                morayClient.createBucket(BENCHMARK_BUCKET_NAME,
                    BENCHMARK_BUCKET_CFG_V0, next);
            },
            function updateBenchmarkBucket(ctx, next) {
                morayClient.updateBucket(BENCHMARK_BUCKET_NAME,
                    BENCHMARK_BUCKET_CFG_V1, next);
            },
            function reindexObjects(ctx, next) {
                morayTools.reindexObjects(morayClient, BENCHMARK_BUCKET_NAME, {
                    reindexCount: 100
                }, next);
            },
            function addSentinelObjects(ctx, next) {
                console.log('Adding %d sentinel objects...', NB_OBJECTS_SENTINEL);

                morayTools.addObjects(morayClient, BENCHMARK_BUCKET_NAME, {
                    reindexed_string:
                        mockedData.getSentinelValueForType('string'),
                    reindexed_number:
                        mockedData.getSentinelValueForType('number'),
                    reindexed_boolean:
                        mockedData.getSentinelValueForType('boolean')
                }, NB_OBJECTS_SENTINEL, next);
            },
            function addNonSentinelObjects(ctx, next) {
                console.log('Adding %d non-sentinel objects...',
                    NB_OBJECTS_NON_SENTINEL);

                morayTools.addObjects(morayClient, BENCHMARK_BUCKET_NAME, {
                    reindexed_string:
                        mockedData.getNonSentinelValueForType('string'),
                    reindexed_number:
                        mockedData.getNonSentinelValueForType('number'),
                    reindexed_boolean:
                        mockedData.getNonSentinelValueForType('boolean')
                }, NB_OBJECTS_NON_SENTINEL, next);
            },
            function searchOnUnindexedString(ctx, next) {
                var filter = '(&(uuid=*)(reindexed_string=' +
                    mockedData.getSentinelValueForType('string')  +  '))';

                morayTools.searchForObjects(morayClient, BENCHMARK_BUCKET_NAME,
                    filter, findobjectsOpts, {
                        nbObjectsExpected: NB_OBJECTS_SENTINEL,
                        expectedProperties: [
                            {
                                name: 'reindexed_string',
                                value: 'foo'
                            }
                        ]
                    }, next);
            },
            function searchOnUnindexedBoolean(ctx, next) {
                var filter = '(&(uuid=*)(reindexed_boolean=' +
                    mockedData.getSentinelValueForType('boolean') + '))'
                morayTools.searchForObjects(morayClient, BENCHMARK_BUCKET_NAME,
                    filter, findobjectsOpts, {
                        nbObjectsExpected: NB_OBJECTS_SENTINEL,
                        expectedProperties: [
                            {
                                name: 'reindexed_boolean',
                                value: true
                            }
                        ]
                    }, next);
            },
            function searchOnUnindexedNumber(ctx, next) {
                var filter = '(&(uuid=*)(reindexed_number=' +
                    mockedData.getSentinelValueForType('number') + '))';
                morayTools.searchForObjects(morayClient, BENCHMARK_BUCKET_NAME,
                    filter, findobjectsOpts, {
                        nbObjectsExpected: NB_OBJECTS_SENTINEL,
                        expectedProperties: [
                            {
                                name: 'reindexed_number',
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
}