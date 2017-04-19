var assert = require('assert-plus');

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

module.exports = {
    getSentinelValueForType: getSentinelValueForType,
    getNonSentinelValueForType: getNonSentinelValueForType
};