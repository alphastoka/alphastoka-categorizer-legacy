var prettyjson = require('prettyjson');
var async = require('async');
var _ = require('lodash');
var fs = require('fs');
var client = require('mongodb').MongoClient;
var array = [];
var picker = [];

var finder = fs.readFileSync('mapper.txt', 'utf8').split('\r\n');

_.forEach(finder, function(e) {
	var r = e.split('|');
	picker[r[0]] = r[1].split(',');
	console.log(picker[r[0]]);
});


var determineCategory = function(content) {
	fs.writeFileSync('test.txt', content, 'utf-8')
	var c = content.toLowerCase();
	var type = null;
	_.forOwn(picker, function(v,k) {
		_.forEach(v, function(e) {
			if(c.indexOf(e) >= 0) {
				type = k;
				return k;
			}
		});
	});
	return type;
};

var getContent = function(doc) {
	var str = '';
	_.forEach(doc.media.nodes, function(e) {
		str += e.caption;
	});
	str += doc.biography;
	return str;
}

// connect to mongo
var url = 'mongodb://54.169.89.105:27017/stoka_Default'
client.connect(url, function(err, db) {
	if(err) {
		console.log(err);
		return;
	}
	var col = db.collection('human');

	console.log('finding all entries');
	async.waterfall([
		function(cb) {
			col.find({}).toArray(function(err, items) {
				if(err) {
					cb(err);
					return;
				}
				cb(null, items);
			})
		},
		function(items, cb) {
			var p = db.collection('human_v2')
			if(p) {
				console.log('dropping');
				p.drop();
			}
			console.log('creating');
			db.createCollection('human_v2');
			cb(null, db.collection('human_v2'), items);
		},
		function(p, items, cb) {
			for(var i = 0 ; i < items.length; i++) {
				items[i].category = determineCategory(getContent(items[i]));
			}
			console.log('inserting');
			p.insertMany(items);
			cb(items);
		}
	], function(err) {
		console.log(err);
	})
})