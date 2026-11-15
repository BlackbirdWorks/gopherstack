'use strict';
// Gopherstack runtime shim: tags console output with the request that produced it, as the managed Node.js runtime does.
const { AsyncLocalStorage } = require('node:async_hooks');
const http = require('node:http');
const util = require('node:util');

const store = new AsyncLocalStorage();
const emit = http.Server.prototype.emit;

http.Server.prototype.emit = function (type, req, ...rest) {
  if (type === 'request' && req && req.headers) {
    const ctx = {
      requestId: req.headers['lambda-web-request-id'],
      endpointName: req.headers['lambda-web-endpoint-name'],
    };
    return store.run(ctx, () => emit.call(this, type, req, ...rest));
  }
  return emit.call(this, type, req, ...rest);
};

// The managed runtime's server accepts at most 1,000 header lines; an app may raise server.maxHeadersCount.
const createServer = http.createServer;
http.createServer = function (...args) {
  const server = createServer.apply(this, args);
  server.maxHeadersCount = 1000;
  return server;
};

const levels = { log: 'INFO', info: 'INFO', debug: 'DEBUG', warn: 'WARN', error: 'ERROR', trace: 'TRACE' };

for (const [method, level] of Object.entries(levels)) {
  console[method] = (...args) => {
    const ctx = store.getStore() || {};
    const single = args.length === 1 && args[0] !== null && typeof args[0] === 'object' && !(args[0] instanceof Error);
    let line;
    try {
      line = JSON.stringify({
        __lambdaweb: 1,
        level,
        message: single ? args[0] : util.format(...args),
        requestId: ctx.requestId,
        endpointName: ctx.endpointName,
      });
    } catch {
      line = JSON.stringify({ __lambdaweb: 1, level, message: util.inspect(args), requestId: ctx.requestId });
    }
    (level === 'WARN' || level === 'ERROR' || level === 'TRACE' ? process.stderr : process.stdout).write(line + '\n');
  };
}
