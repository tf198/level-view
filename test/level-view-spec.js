const expect = require('chai').expect;

const LevelUp = require('levelup');
const MemDown = require('memdown');
const bytewise = require('bytewise');
const concat = require('concat-stream');
const async = require('async');

const LevelView = require('..');

var _db = 0;

function testDB(options) {
    MemDown.clearGlobalStore();
    options = Object.assign({db: MemDown, name: 'levelview'}, options);
    return LevelUp(options.name + '-' + (_db++), options);

}

const TEST_DATA = [
    {key: 1, value: {firstName: 'Bob', lastName: 'Brown', age: 17, height: 176}},
    {key: 2, value: {firstName: 'Andy', lastName: 'Andrews', age: 23, height: 180}},
    {key: 3, value: {firstName: 'Charlie', lastName: 'Chaplin', age: 34, height: 182}},
    {key: 4, value: {firstName: 'Dave', age: 14, height: 185}},
    {key: 5, value: {firstName: 'Evan', age: 14, height: 176}}

];

describe('LevelView', function() {
    it('should handle simple values', function(done) {
        var v = new LevelView(testDB());

        v.addView('firstName', function(key, value, emit) {
            emit(value.firstName, null);  
        });

        v.addView('lastName', function(key, value, emit) {
            emit(value.lastName, null);
        });

        v.indexMany(TEST_DATA, (err) => {
            async.parallel([
                (next) => {
                    v.query('firstName', {key: 'Charlie'}, (err, data) => {
                        expect(data.map((x) => x.doc)).to.eql([3]);
                        next(err);
                    });
                },
                (next) => {
                    v.query('firstName', {startkey: 'B', endkey: 'D'}, (err, data) => {
                        expect(data.map((x) => x.doc)).to.eql([1, 3]);
                        next(err);
                    });
                },
                (next) => {
                    v.query('lastName', 'Brown', (err, data) => {
                        expect(data.map((x) => x.doc)).to.eql([1]);
                        next(err);
                    });
                },
                (next) => {
                    v.query('lastName', (err, data) => {
                        expect(data.map((x) => x.key)).to.eql(['Andrews', 'Brown', 'Chaplin']);
                        next(err);
                    });
                }
            ], done);
        });

    });

    it('should handle complex keys', function(done) {
        var v = new LevelView(testDB())

        v.addView('test_1', function(key, value, emit) {
            emit([value.age, value.height], value.firstName);
        });

        v.indexMany(TEST_DATA, (err) => {
            async.parallel([
                (next) => {
                    v.query('test_1', {startkey: [10], endkey: [17, undefined]}, (err, data) => {
                        expect(data.map((x) => x.value)).to.eql(['Evan', 'Dave', 'Bob']);
                        next(err);
                    });
                }
            ], done);

        });
    });

    it('should handle lookups on bad view', function() {
        var v = new LevelView(testDB());

        expect(v.query.bind(v, 'bad_view')).to.throw('No such view: bad_view');
    });

    xit('should apply a reducer', function(done) {
        var v =new LevelView(testDB());

        v.addView('name', function(key, value, emit) {
            emit(value.firstName, value.age);
        });

        var r = LevelView.Reducer((a, b, rereduce) => (rereduce) ? a + b : 2);

        v.indexMany(TEST_DATA, (err) => {

            

            done();
        });
    });
});
