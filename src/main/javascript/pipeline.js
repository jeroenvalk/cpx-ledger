/**
 * Copyright © 2018 dr. ir. Jeroen M. Valk
 *
 * This file is part of ComPosiX. ComPosiX is free software: you can
 * redistribute it and/or modify it under the terms of the GNU Lesser General
 * Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * ComPosiX is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ComPosiX. If not, see <http://www.gnu.org/licenses/>.
 */

_.module('pipeline', ['logger', 'channel', 'pipe', 'source', 'target', 'sourceArray'], function (_, logger, channel, pipe) {
	const path = require('path'), fs = require('fs');

	const source = function (readable) {
		return new Promise(function (resolve, reject) {
			switch (typeof readable) {
				case 'string':
					fs.stat(readable, function (err, stats) {
						if (err) {
							reject(new Error());
							return;
						}
						if (stats.isFile()) {
							resolve({rd: fs.createReadStream(readable)});
							return;
						}
						if (stats.isDirectory()) {
							fs.readdir(readable, function (err, files) {
								resolve({
									rd: _.map(files, function (file) {
										return path.parse([readable, file].join("/"))
									})
								});
							});
							return;
						}
						reject(new Error());
					});
					break;
				default:
					resolve({rd: readable});
					break;
			}
		});
	};

	const target = function (callback, resolve) {
		var n = 0;

		const all = [];

		const eof = function() {
			Promise.all(all).then(function() {
				resolve(n);
			})
		};

		const self = {
			pipeline: function() {
				all.push(pipeline.apply(null, arguments));
			}
		};

		const wrapper = function (value) {
			++n;
			const result = callback.call(self, value);
			if (result) {
				_.each(result, function (value, key) {
					fs.writeFileSync(key, value);
				});
			}
		};
		return Promise.resolve({
			wr: {
				write: wrapper,
				end: eof
			}
		});
	};

	const link = function (node) {
		return Promise.resolve(node);
	};

	const error = function (e) {
		console.error(e);
	};

	const forward = function(array, todo) {
		if (todo.length > 0) {
			const i = todo.pop();
			pipe(array[i - 1], array[i]).then(function(result) {
				array[i] = result;
				forward(array, todo);
			});
		}
	};

	const pipeline = function () {
		const argv = arguments;
		return new Promise(function(resolve) {
			var i, size = argv.length;
			const chain = new Array(size--);

			chain[0] = source(argv[0]);
			chain[0].catch(error);
			for (i = 1; i < size; ++i) {
				chain[i] = link(argv[i]);
				chain[i].catch(error);
			}
			chain[i] = target(argv[i], resolve);
			chain[i].catch(error);

			Promise.all(chain).then(function (array) {
				const todo = [];
				var target;
				while (i > 0) {
					target = array[i--];
					switch(typeof array[i]) {
						case 'function':
							todo.push(i--);
							break;
						case 'object':
							pipe(array[i].rd, target.wr);
							break;
					}
				}
				forward(array, todo);
			}).catch(error);
		});
	};

	return pipeline;
});

