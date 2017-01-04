const Sublevel = require('level-sublevel/bytewise');
const bytewise = require('bytewise');
const util = require('util');
const stream = require('stream');

function ViewTransform(options) {
    if(!this instanceof ViewTransform) return new ViewTransform(d, options);

    if(!options) options = {};
    options.objectMode = true;

    stream.Transform.call(this, options);
}

util.inherits(ViewTransform, stream.Transform);

ViewTransform.prototype._transform = function(obj, encoding, cb) {
    try {
        var parts = obj.key;
        var d = {
            key: parts[1],
            doc: parts[2],
            value: obj.value
        };
        this.push(d);
        cb();
    } catch(e) {
        console.error(obj);
        cb(e);
    }
}

// end ViewTransform

function DocTransform(db, options) {
    if(!this instanceof DocTransform) return new DocTransform(d, options);

    if(!options) options = {};
    options.objectMode = true;

    this.db = db;

    stream.Transform.call(this, options);
}

util.inherits(DocTransform, stream.Transform);

DocTransform.prototype._transform = function(obj, encoding, cb) {
    this.db.get(obj.doc, {valueEncoding: 'json'}, function(err, data) {
        if(err) return cb(err);
        this.push(data);
        cb();
    }.bind(this));
}
// end DocTransform

function Reducer(callback, levels, options) {
    if(!this instanceof Reducer) return new Reducer(callback, levels, options);

    if(typeof(levels) == 'object') return new Reducer(callback, undefined, levels);

    if(!options) options = {};
    options.objectMode = true;

    if(typeof(levels) != 'number') levels = 1;

    this.callback = callback;
    this.levels = levels;

    stream.Transform.call(this, options);
}

util.inherits(Reducer, stream.Transform);

// end Reducer

function LevelView(db) {
    this._db = db;
    this._views = {};
}

LevelView.prototype.addView = function(name, callable) {
    this._views[name] = callable;
}

LevelView.prototype.getView = function(name) {
    if(typeof(this._views[name]) == 'undefined') {
        throw new Error("No such view: " + name);
    }
    return this._views[name];
}

LevelView.prototype.index = function(key, value, cb) {
    this._db.batch(this.createBatch(key, value), cb);
}

LevelView.prototype.indexMany = function(items, cb) {
    var batch = [];
    items.map((item) => {
        batch = batch.concat(this.createBatch(item.key, item.value));
    });
    this._db.batch(batch, cb);
}

LevelView.prototype.createBatch = function(key, value) {
    var self = this;
    var batch = [];

    Object.keys(self._views).map(function(name) {

        function emit(indexKey, indexValue) {
            //console.log(key, value);
            batch.push({
                prefix: self._db,
                type: 'put',
                key: [name, indexKey, key],
                value: indexValue,
                keyEncoding: bytewise,
                valueEncoding: bytewise
            });
        }

        self.getView(name)(key, value, emit);
    });
    return batch;
}

LevelView.prototype.createQuery = function(name, query) {

    // check it exists
    this.getView(name);

    if(typeof(query) == 'string') query = {key: query};

    query = query || {};
    
    var gt = [name];
    var lt = [name];

    if(typeof(query.key) != 'undefined') {
        query.startkey = query.key;
        query.endkey = query.key;
    }
        
    if(typeof(query.startkey) != 'undefined') gt.push(query.startkey);
    if(typeof(query.endkey) != 'undefined') lt.push(query.endkey);

    lt.push(undefined);

    return {gt, lt, keyEncoding: bytewise, valueEncoding: bytewise};
}

LevelView.prototype.createQueryStream = function(name, query) {
    return this._db.createReadStream(this.createQuery(name, query)).pipe(new ViewTransform());
}

LevelView.prototype.query = function(name, query, cb) {

    if(typeof(query) == 'function') return this.query(name, undefined, query);

    var result = [];
    this.createQueryStream(name, query)
        .on('data', (data) => result.push(data))
        .on('error', cb)
        .on('end', () => cb(null, result))
}

LevelView.prototype.createDocStream = function(db, name, query) {
    return this.createQueryStream(name, query).pipe(new DocTransform(db));
}

LevelView.Reducer = Reducer;
LevelView.DocTransform = DocTransform;

module.exports = LevelView;
