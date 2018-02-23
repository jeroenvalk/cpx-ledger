/**
 * Copyright © 2016-2017 dr. ir. Jeroen M. Valk
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

module.exports = function (_) {
	'use strict';

	const cpx_path = require('composix')(_);
	cpx_path.push(__dirname);

	_.require('module')(_);

	_.module('logger', function () {
		return {
			error: console.error
		};
	});

	_.module('pipeline', ['logger', 'channel', 'pipe'], function (_, logger, channel, pipe) {
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

	_.module('ledgerNormalize', ['channel', 'pipe'], function (_, channel, pipe) {
		const i = channel.create(true), o = channel.create(true);

		const whitespace = function (str) {
			return str.split(/\s+/).join(" ");
		};

		const recurse = function (object) {
			const result = {};
			var i, todo;
			switch (typeof object) {
				case 'string':
				case 'number':
					object = [object];
				/* falls through */
				case 'object':
					if (object === null) {
						return [null];
					}
					if (object instanceof Array) {
						for (i = 0; i < object.length; ++i) {
							switch (typeof object[i]) {
								case 'object':
									if (object[i] !== null) {
										throw new Error("use null to mark a balancing account");
									}
									break;
								case 'string':
									switch (todo) {
										case undefined:
											todo = true;
											break;
										case false:
											throw new Error();
									}
									break;
								case 'number':
									switch (todo) {
										case undefined:
											todo = false;
											break;
										case true:
											throw new Error();
									}
									break;
							}
						}
						if (todo) {
							_.each(object, function (value) {
								if (value === null) {
									if (result._ === null) {
										throw new Error("only one posting with null amount allowed per transaction");
									}
									result[''] = null;
								} else {
									const part = value.split(/\s+/);
									if (isNaN(part[0])) {
										throw new Error();
									}
									if (!isNaN(part[1])) {
										throw new Error();
									}
									if (!result[part[1]]) {
										result[part[1]] = [];
									}
									result[part[1]].push(parseFloat(part[0]));
								}
							});
						}
					} else {
						_.each(object, function (value, key) {
							switch (key) {
								case '$':
									result.$ = value;
									break;
								default:
									_.set(result, _.map(key.split(":"), whitespace), recurse(value));
									break;
							}
						});
					}
					return result;
				default:
					throw new Error(JSON.stringify(object));
			}
		};

		pipe(i.rd, {
			write: function (value) {
				const result = {};
				_.each(value, function (value, key) {
					if (!(value instanceof Array)) {
						value = [value];
					}
					result[new Date(key).toISOString()] = _.map(value, recurse);
				});
				channel.write(o.wr, result);
			},
			end: function() {
				channel.write(o.wr, null);
			}
		});

		return {
			rd: o.rd,
			wr: i.wr
		};
	});

	_.module("flatten", ["channel", "pipe"], function (_, channel, pipe) {
		const ch = channel.create(true);

		pipe(ch.rd, {
			write: function (ch) {
				pipe(ch.rd, {
					write: function (chunk) {
						channel.write(ch.wr, chunk);
					}
				})
			},
			end: function() {

			}
		});

		return ch.wr;
	});

	_.module("stdout", ["channel", "pipe"], function (_, channel, pipe) {
		const ch = channel.create();
		pipe(ch.rd, {
			write: function (chunk) {
				process.stdout.write(chunk);
			},
			end: function () {
			}
		});
		return ch.wr;
	});

	_.module("ledgerExport", ["channel", "pipe"], function (_, channel, pipe) {
		const i = channel.create(true), o = channel.create();

		pipe(i.rd, {
			write: function (result) {
				process.nextTick(function () {
					const toledger = function (object, path) {
						_.each(object, function (value, key) {
							switch (key) {
								case "$":
									break;
								case "":
									if (value !== null) {
										throw new Error();
									}
									channel.write(o.wr, Buffer.from("\t" + path.join(":") + "\n"));
									break;
								default:
									if (_.isArray(value)) {
										_.each(value, function (value) {
											channel.write(o.wr, Buffer.from("\t" + path.join(":") + "\t" + value + " " + key + "\n"));
										});
									} else {
										path.push(key);
										toledger(value, path);
										path.pop();
									}
									break;
							}
						});
					};

					_.each(result, function (value, key) {
						var line = [key.substr(0, 10), key.substr(12)];
						if (!_.isArray(value)) {
							throw new Error();
						}
						_.each(value, function (value) {
							if (_.has(value, "$")) {
								line.push(value.$);
							}
							channel.write(o.wr, Buffer.from(line.join(" ") + "\n"));
							toledger(value, []);
						});
					});

					channel.write(o.wr, null);
				});
			},
			end: function() {
				channel.write(o.wr, null);
			}
		});

		return {
			rd: o.rd,
			wr: i.wr
		};
	});

	_.module("csv", ["channel", "pipe"], function (_, channel, pipe) {
		const ch = channel.create(true);
		pipe(ch.rd, {
			write: function (x) {
				const parser = require('csv-parse')(x.config || {columns: true});
				parser.on("readable", function () {
					var record;
					while (record = parser.read()) {
						channel.write(x.wr, record);
					}
				});
				parser.on("error", function (err) {
					throw err;
				});
				parser.on("finish", function () {
					channel.write(x.wr, null);
				});
				pipe(x.rd, parser);
			},
			end: function() {
				throw new Error("cannot close csv service stream");
			}
		});
		return ch.wr;
	});

	_.module("importKraken", ["channel", "pipe", "csv", "ledgerNormalize"], function (_, channel, pipe, csv, ldgr) {
		const ch = channel.create(), i = channel.create(true), o = channel.create(true);
		var result = {};

		const updater = function (result) {
			return function (value) {
				return value ? _.concat(value, result) : result;
			};
		};

		pipe(i.rd, {
			write: function (record) {
				var asset;
				const timestamp = new Date(record.time).toISOString()
				const amount = parseFloat(record.amount);
				const fee = parseFloat(record.fee);
				switch (record.asset) {
					case "ZEUR":
						asset = "EUR";
						break;
					case "XXBT":
						asset = "BTC";
						break;
					case "DASH":
						asset = "DASH";
						break;
					case "XXLM":
						asset = "XLM";
						break;
					case "XXMR":
						asset = "XMR";
						break;
					case "XLTC":
						asset = "LTC";
						break;
					case "XETH":
						asset = "ETH";
						break;
					case "XXRP":
						asset = "XRP";
						break;
					case "XETC":
						asset = "ETC";
						break;
					case "BCH":
						asset = "BCH";
						break;
					case "XREP":
						asset = "REP";
						break;
					case "XXDG":
						asset = "XDG";
						break;
					default:
						throw new Error(record.asset);
				}
				_.update(result, [timestamp, "Assets:Crypto:Exchange:Kraken:Trading"], updater([amount - fee, asset].join(" ")));
				if (fee > 0) _.update(result, [timestamp, "Expenses:Crypto:Exchange:Kraken:Fees"], updater([fee, asset].join(" ")));
			},
			end: function () {
				_.each(result, function (value) {
					const amounts = value["Assets:Crypto:Exchange:Kraken:Trading"];
					if (!(amounts instanceof Array)) {
						value["Assets:Crypto:Exchange:Kraken:Funding"] = null;
					}
				});
				channel.write(o.wr, result);
				result = {};
			}
		});

		channel.write(csv, {
			rd: ch.rd,
			wr: i.wr
		});

		return {
			wr: ch.wr,
			rd: o.rd
		};
	});

	return cpx_path;
};
