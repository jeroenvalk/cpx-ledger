_.module("pipe", ["channel"], function (_, channel) {

	const readChannel = function (rd, callback) {
		const recurse = function () {
			channel.read(rd, 1, function (array) {
				callback(array);
				if (array.length > 0) {
					recurse();
				}
			});
		};
		recurse();
	};

	const readStream = function (readable, callback) {
		if (readable instanceof Array) {
			for (var i = 0; i < readable.length; ++i) {
				callback([readable[i]]);
			}
			callback([]);
			return;
		}
		readable.on("data", function (chunk) {
			callback([chunk]);
		});
		readable.on("end", function () {
			callback([]);
		});
		readable.resume();
	};

	const writeChannel = function (wr, array) {
		if (array.length > 0) {
			channel.write(wr, array[0]);
		} else {
			channel.write(wr, null);
		}
	};

	const writeStream = function (writable, array) {
		if (array.length > 0) {
			writable.write(array[0]);
		} else {
			writable.end();
		}
	};

	return function (source, target) {
		(isFinite(source) ? readChannel : readStream)(source, _.curry(isFinite(target) ? writeChannel : writeStream)(target));
	};
});

