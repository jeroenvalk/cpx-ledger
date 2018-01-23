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

	_ = require('composix')(_);

	_.module("pipe", ["channel"], function (channel) {
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

	_.module('pipeline', ['pipe'], function (pipe) {
		const path = require('path'), fs = require('fs');
		return function (source) {
			var size = arguments.length;
			const argv = arguments, callback = argv[--size];

			const wrapper = function (value) {
				const result = callback(value);
				if (result) {
					_.each(result, function (value, key) {
						fs.writeFileSync(key, value);
					});
				}
			};

			const closure = function () {
				var target = {
					write: wrapper,
					end: function () {
					}
				};
				while (size > 1) {
					pipe(argv[--size].rd, target);
					target = argv[size].wr;
				}
				pipe(source, target);
			};

			switch (typeof source) {
				case 'string':
					fs.stat(source, function (err, stats) {
						if (err) {
							throw err;
						}
						if (stats.isFile()) {
							source = fs.createReadStream(source);
							closure();
						} else if (stats.isDirectory()) {
							fs.readdir(source, function (err, files) {
								source = _.map(files, function (file) {
									return path.parse([source, file].join("/"))
								});
								closure();
							});
						} else {
							throw new Error();
						}
					});
					break;
				default:
					closure();
					break;
			}
		};
	});

	_.module('ledgerNormalize', ['channel', 'pipe'], function (channel, pipe) {
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
			}
		});

		return {
			rd: o.rd,
			wr: i.wr
		};
	});

	_.module("flatten", ["channel", "pipe"], function (channel, pipe) {
		const ch = channel.create(true);

		pipe(ch.rd, {
			write: function (ch) {
				pipe(ch.rd, {
					write: function (chunk) {
						channel.write(ch.wr, chunk);
					}
				})
			}
		});

		return ch.wr;
	});

	_.module("stdout", ["channel", "pipe"], function (channel, pipe) {
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

	_.module("ledgerExport", ["channel", "pipe"], function (channel, pipe) {
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
			}
		});

		return {
			rd: o.rd,
			wr: i.wr
		};
	});

	_.module("csv", ["channel", "pipe"], function (channel, pipe) {
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
			}
		});
		return ch.wr;
	});

	_.module("importKraken", ["channel", "pipe", "csv", "ledgerNormalize"], function (channel, pipe, csv, ldgr) {
		const ch = channel.create(), i = channel.create(true), o = channel.create(true);
		var result = {};

		const updater = function(result) {
			return function(value) {
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
				_.update(result, [timestamp, "Assets:Crypto:Exchange:Kraken:Holdings"], updater([amount - fee, asset].join(" ")));
				if (fee > 0) _.update(result, [timestamp, "Expenses:Crypto:Exchange:Kraken:Fees"], updater([fee, asset].join(" ")));
			},
			end: function () {
				_.each(result, function (value) {
					const amounts = value["Assets:Crypto:Exchange:Kraken:Holdings"];
					if (!(amounts instanceof Array)) {
						if (parseInt(amounts.split(" ")[0]) < 0) {
							value["Assets:Crypto:Exchange:Kraken:Withdrawals"] = null;
						} else {
							value["Assets:Crypto:Exchange:Kraken:Deposits"] = null;
						}
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

	return _;
};
