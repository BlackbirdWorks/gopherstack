package cloudfront

// fnPrelude defines the modules a CloudFront Function may require. __hash,
// __hmac, __allowed and (runtime 2.0) __kvs are bound from Go first.
const fnPrelude = `
var __mod_crypto = {
  createHash: function (alg) {
    var data = '';
    return { update: function (s) { data += s; return this; },
             digest: function (enc) { return __hash(alg, data, enc || 'hex'); } };
  },
  createHmac: function (alg, key) {
    var data = '';
    return { update: function (s) { data += s; return this; },
             digest: function (enc) { return __hmac(alg, key, data, enc || 'hex'); } };
  }
};
var __mod_qs = {
  parse: function (s) {
    var out = {};
    String(s || '').split('&').forEach(function (p) {
      if (!p) { return; }
      var i = p.indexOf('='), k = decodeURIComponent(i < 0 ? p : p.slice(0, i)),
          v = i < 0 ? '' : decodeURIComponent(p.slice(i + 1));
      if (out[k] === undefined) { out[k] = v; }
      else if (Array.isArray(out[k])) { out[k].push(v); }
      else { out[k] = [out[k], v]; }
    });
    return out;
  },
  stringify: function (o) {
    var parts = [];
    Object.keys(o || {}).forEach(function (k) {
      [].concat(o[k]).forEach(function (v) { parts.push(encodeURIComponent(k) + '=' + encodeURIComponent(v)); });
    });
    return parts.join('&');
  }
};
var __mod_cf = typeof __kvs === 'function' ? { kvs: __kvs } : undefined;
var __mods = { crypto: __mod_crypto, querystring: __mod_qs, cloudfront: __mod_cf };
function require(name) {
  if (__allowed.indexOf(name) < 0 || !__mods[name]) { throw new Error("Cannot find module '" + name + "'"); }
  return __mods[name];
}
`
