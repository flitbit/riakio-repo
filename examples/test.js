var util = require('util')
;

var 
them = []
;

function one(inputs, next) {
	console.log(this.name.concat('- one got: ', util.inspect(inputs, false, 99)));

	next(null, "called one ".concat(util.inspect(inputs, false, 99), '\n'));
}

function two(inputs, next) {
	console.log(this.name.concat('- two got: ', util.inspect(inputs, false, 99)));

	next(null, "called two ".concat(util.inspect(inputs, false, 99), '\n'));
}

them.push(one.toString());
them.push(two.toString());

function It(pipe, name) {
	var self = this;
	this.name = name;
	this.pipe = pipe.map(function(it) { 
		eval('var f = '.concat(it));
		return f.bind(self);
	});

	this.do = function(inputs, callback) {
		var inp = inputs
		;
		this.pipe.forEach(function(it) {
			it(inp, function(err, res) {
				if (err) callback(err);
				else {
					inp = res;
				}
			});
		});
		callback(null, "final ".concat(util.inspect(inp, false, 99)));
	}
}

var it = new It(them, "you");
it.do("stuff", function(err, res) {
	console.log(util.inspect(err || res, false, 99));
});


