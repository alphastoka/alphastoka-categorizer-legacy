var mlab = require('mongolab-data-api')('ucQuRaICqjzsxmtTVyuXp3dxzNheiKmy');
var prettyjson = require('prettyjson');
var async = require('async');
var _ = require('lodash');
var fs = require('fs');
var opts = {
	database: 'alphastoka',
	collectionName: 'profilesv5_b',
	limit: 10000000
	//resultCount: true
}
var opts2 = {
	database: 'alphastoka',
	collectionName: 'profilesv5_c',
	resultCount: true
}

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

mlab.listDocuments(opts, function(err, data) {
	if(err) {
		console.log(err);
		return;
	}
	array = data;
	console.log('data ' + array.length + ' entries found');

	for(var i = 0; i < array.length; i++) {
		array[i].category = determineCategory(array[i].description);
		console.log(array[i].category);
	}
	async.waterfall([
		function(cb) {
			mlab.listDocuments(opts2, function(err) {
				if(err) {
					console.log(opts2.collectionName + ' not exist');
					cb(null, true);
				}
				console.log(opts2.collectionName + ' exist');
				cb(null, false);
			});
		},
		function(create, cb) {
			if(!create) {
				console.log(opts2.collectionName + ' deleting');
				mlab.deleteDocuments(opts2, function(err) {
					if(err) {
						return cb(err);
					}
					cb();
				})
			}
			else {
				cb();
			}
		}, 
		function(cb) {
			var d = _.chunk(array, 10);
			async.map(d, function(i, cb2) {
				console.log(opts2.collectionName + ' inserting ' + i.length + ' entries');
				var o = _.extend({}, opts2, {
					documents: i
				});
				mlab.insertDocuments(o, cb2);
			}, cb);
		}
	], function(err) {
		if(err) {
			console.log(err);
			return;
		}
		console.log('finished');
	})
});