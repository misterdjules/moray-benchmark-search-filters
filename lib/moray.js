var assert = require('assert-plus');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var util = require('util');
var vasync = require('vasync');

function addObjects(morayClient, bucketName, objectTemplate, nbObjects,
    callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.object(objectTemplate, 'objectTemplate');
    assert.number(nbObjects, 'nbObjects');
    assert.func(callback, 'callback');

    var totalNbObjectsCreated = 0;
    var ADD_CONCURRENCY = 100;

    function _addObjects() {
        var i = 0;
        var keys = [];
        var nbObjectsToCreate =
            Math.min(nbObjects - totalNbObjectsCreated, ADD_CONCURRENCY);

        if (nbObjectsToCreate === 0) {
            callback();
            return;
        }

        for (i = 0; i < nbObjectsToCreate; ++i) {
            keys.push(libuuid.create());
        }

        vasync.forEachParallel({
            func: function addObject(key, done) {
                var objectData = jsprim.deepCopy(objectTemplate);

                objectData.uuid = key;

                morayClient.putObject(bucketName, key, objectData,
                    function onObjectAdded(addErr) {
                        var nonTransientErrorNames = [
                            'InvalidIndexTypeError',
                            'UniqueAttributeError'
                        ];

                        if (addErr &&
                            nonTransientErrorNames.indexOf(addErr.name)
                                !== -1) {
                            done(addErr);
                            return;
                        }

                        if (!addErr) {
                            ++totalNbObjectsCreated;
                        }

                        done();
                    });
            },
            inputs: keys
        }, function onObjectsAdded(err) {
            if (err) {
                callback(err);
            } else {
                setImmediate(_addObjects);
            }
        });
    }

    _addObjects();
}

function reindexObjects(morayClient, bucketName, options, callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketname');
    assert.object(options, 'options');
    assert.number(options.reindexCount, 'options.reindexCount');
    assert.func(callback, 'callback');

    function _reindex() {
        morayClient.reindexObjects(bucketName, options.reindexCount,
            function onObjectsReindex(reindexErr, count) {
                if (reindexErr) {
                    callback(reindexErr);
                    return;
                } else {
                    if (count.remaining === 0) {
                        callback();
                        return;
                    } else {
                        setImmediate(_reindex);
                    }
                }
            });
    }

    _reindex();
}

function findObjectsWithFilter(morayClient, bucketName, filter, options, callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.string(filter, 'filter');
    assert.object(options, 'options');
    assert.optionalNumber(options.requiredBucketVersion,
        'options.requiredBucketVersion');
    assert.func(callback, 'callback');

    var objects = [];
    var req;

    req = morayClient.findObjects(bucketName, filter, {
        requiredBucketVersion: options.requiredBucketVersion,
        noLimit: true
    });

    req.on('error', function onError(err) {
        callback(err, objects);
    });

    req.on('end', function onEnd() {
        callback(null, objects);
    });

    req.on('record', function onRecord(obj) {
        objects.push(obj);
    });
}

function searchForObjects(morayClient, bucketName, filter, options, expectedResults, callback) {
    assert.object(morayClient, 'morayClient');
    assert.string(bucketName, 'bucketName');
    assert.string(filter, 'filter');
    assert.object(options, 'options');
    assert.object(expectedResults, 'expectedResults');
    assert.optionalNumber(expectedResults.nbObjectsExpected,
        'expectedResults.nbObjectsExpected');
    assert.func(callback, 'callback');

    console.log('searching objects with filter [%s] and options [%j]',
        filter, options);

    findObjectsWithFilter(morayClient, bucketName, filter, options,
        function objectsFound(err, objectsFound) {
            var nbObjectsExpected = expectedResults.nbObjectsExpected || 0;
            var nbObjectsFound = 0;

            assert.number(nbObjectsExpected, nbObjectsExpected);

            if (err) {
                callback(err);
                return;
            }

            if (objectsFound !== undefined) {
                nbObjectsFound = objectsFound.length;
            }

            console.log(util.format('%d/%d objects found', nbObjectsFound,
                    nbObjectsExpected));

            if (expectedResults.expectedProperties) {
                expectedResults.expectedProperties.forEach(function (expectedProperty) {
                    var expectedPropertyName = expectedProperty.name;
                    var expectedPropertyValue = expectedProperty.value;
                    var allValuesMatch = false;

                    assert.string(expectedPropertyName,
                        'expectedPropertyName');

                    allValuesMatch =
                        objectsFound.some(function checkObject(object) {
                        var value = object.value;
                        return value[expectedPropertyName] ===
                            expectedPropertyValue;
                    });

                    if (allValuesMatch) {
                        console.log('all values for property ' +
                            expectedPropertyName + ' match expected value ' +
                            expectedPropertyValue);
                    } else {
                        console.log('some values for property ' +
                            expectedPropertyName + ' do not match expected ' +
                            'value ' + expectedPropertyValue);
                    }
                });
            }

            callback();
        });
}

module.exports = {
    addObjects: addObjects,
    reindexObjects: reindexObjects,
    findObjectsWithFilter: findObjectsWithFilter,
    searchForObjects: searchForObjects
};