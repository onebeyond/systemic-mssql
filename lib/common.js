module.exports.sequential = (func, args) =>
	args.reduce(
		(promise, arg) => promise.then(result => func(...arg).then(Array.prototype.concat.bind(result))),
		Promise.resolve([]),
	);
