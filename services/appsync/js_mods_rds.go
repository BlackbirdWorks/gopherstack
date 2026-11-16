package appsync

// jsPreludeRDS defines the @aws-appsync/utils/rds module: the sql tag, the
// select/insert/update/remove builders, statement creators and result helpers.
const jsPreludeRDS = `
var __mod_rds = (function (util) {
  var typeHint = {};
  ['DECIMAL', 'TIMESTAMP', 'TIME', 'DATE', 'UUID', 'JSON'].forEach(function (h) {
    typeHint[h] = function (v) { return { __hint: h, value: v }; };
  });
  function sql(strings) {
    return { strings: Array.prototype.slice.call(strings), values: Array.prototype.slice.call(arguments, 1) };
  }
  function builder(d) {
    var b = { strings: [''], values: [], names: [], d: d };
    b.text = function (s) { b.strings[b.strings.length - 1] += s; return b; };
    b.bind = function (v, name) { b.values.push(v); b.names.push(name); b.strings.push(''); return b; };
    b.id = function (s) { return b.text(ident(d, s)); };
    return b;
  }
  function quote(d, part) {
    var q = d === 'mysql' ? String.fromCharCode(96) : '"';
    return q + String(part).split(q).join(q + q) + q;
  }
  function ident(d, name) {
    return String(name).split('.').map(function (p) { return p === '*' ? p : quote(d, p); }).join('.');
  }
  var binary = { eq: '=', ne: '<>', gt: '>', ge: '>=', lt: '<', le: '<=' };
  function list(b, field, kw, vals) {
    b.id(field).text(' ' + kw + ' (');
    vals.forEach(function (x, i) { if (i) { b.text(', '); } b.bind(x, field); });
    b.text(')');
  }
  function sizeCond(b, field, v) {
    b.text('LENGTH(').id(field).text(')');
    if (v !== null && typeof v === 'object') {
      var op = Object.keys(v)[0];
      if (!binary[op]) { throw new Error('unsupported size operator: ' + op); }
      b.text(' ' + binary[op] + ' ').bind(v[op], field);
    } else { b.text(' = ').bind(v, field); }
  }
  function cond(b, field, op, v) {
    if (binary[op]) { b.id(field).text(' ' + binary[op] + ' ').bind(v, field); }
    else if (op === 'in') { list(b, field, 'IN', v); }
    else if (op === 'notIn') { list(b, field, 'NOT IN', v); }
    else if (op === 'between') { b.id(field).text(' BETWEEN ').bind(v[0], field).text(' AND ').bind(v[1], field); }
    else if (op === 'beginsWith') { b.id(field).text(' LIKE ').bind(v + '%', field); }
    else if (op === 'contains') { b.id(field).text(' LIKE ').bind('%' + v + '%', field); }
    else if (op === 'notContains') { b.id(field).text(' NOT LIKE ').bind('%' + v + '%', field); }
    else if (op === 'attributeExists') { b.id(field).text(v ? ' IS NOT NULL' : ' IS NULL'); }
    else if (op === 'size') { sizeCond(b, field, v); }
    else { throw new Error('unsupported where operator: ' + op); }
  }
  function group(b, subs, joiner) {
    subs.forEach(function (sub, i) {
      if (i) { b.text(joiner); }
      if (subs.length > 1) { b.text('('); }
      where(b, sub);
      if (subs.length > 1) { b.text(')'); }
    });
  }
  function where(b, w) {
    Object.keys(w).forEach(function (k, n) {
      if (n) { b.text(' AND '); }
      var v = w[k];
      if (k === 'and' || k === 'or') {
        b.text('('); group(b, Array.isArray(v) ? v : [v], k === 'and' ? ' AND ' : ' OR '); b.text(')');
      } else if (k === 'not') {
        b.text('NOT ('); group(b, Array.isArray(v) ? v : [v], ' AND '); b.text(')');
      } else {
        Object.keys(v).forEach(function (op, i) { if (i) { b.text(' AND '); } cond(b, k, op, v[op]); });
      }
    });
  }
  function rcols(r) { return (r === true || r === '*') ? null : (typeof r === 'string' ? [r] : r); }
  function cols(b, c) {
    if (!c || !c.length) { b.text('*'); return; }
    c.forEach(function (x, i) { if (i) { b.text(', '); } b.id(x); });
  }
  function selectStmt(o, d) {
    var b = builder(d).text('SELECT ');
    cols(b, rcols(o.columns));
    b.text(' FROM ').id(o.table);
    if (o.where) { b.text(' WHERE '); where(b, o.where); }
    if (o.orderBy && o.orderBy.length) {
      b.text(' ORDER BY ');
      o.orderBy.forEach(function (x, i) {
        if (i) { b.text(', '); }
        b.id(x.column);
        if (x.dir) { b.text(' ' + x.dir); }
      });
    }
    if (o.limit !== undefined) { b.text(' LIMIT ').bind(o.limit, 'limit'); }
    if (o.offset !== undefined) { b.text(' OFFSET ').bind(o.offset, 'offset'); }
    return b;
  }
  function eqWhere(values) {
    var w = {};
    Object.keys(values).forEach(function (k) { w[k] = { eq: values[k] }; });
    return w;
  }
  function returning(b, r) { b.text(' RETURNING '); cols(b, rcols(r)); return b; }
  function select(o) { return { __build: function (d) { return [selectStmt(o, d)]; } }; }
  function insert(o) {
    return { __build: function (d) {
      var keys = Object.keys(o.values), b = builder(d);
      b.text('INSERT INTO ').id(o.table).text('(');
      keys.forEach(function (k, i) { if (i) { b.text(', '); } b.id(k); });
      b.text(') VALUES(');
      keys.forEach(function (k, i) { if (i) { b.text(', '); } b.bind(o.values[k], k); });
      b.text(')');
      if (!o.returning) { return [b]; }
      if (d === 'pg') { return [returning(b, o.returning)]; }
      return [b, selectStmt({ table: o.table, columns: rcols(o.returning), where: eqWhere(o.values) }, d)];
    } };
  }
  function update(o) {
    return { __build: function (d) {
      var keys = Object.keys(o.values), b = builder(d).text('UPDATE ').id(o.table).text(' SET ');
      keys.forEach(function (k, i) { if (i) { b.text(', '); } b.id(k).text(' = ').bind(o.values[k], k); });
      if (o.where) { b.text(' WHERE '); where(b, o.where); }
      if (!o.returning) { return [b]; }
      if (d === 'pg') { return [returning(b, o.returning)]; }
      return [b, selectStmt({ table: o.table, columns: rcols(o.returning), where: o.where }, d)];
    } };
  }
  function remove(o) {
    return { __build: function (d) {
      var b = builder(d).text('DELETE FROM ').id(o.table);
      if (o.where) { b.text(' WHERE '); where(b, o.where); }
      if (!o.returning) { return [b]; }
      if (d === 'pg') { return [returning(b, o.returning)]; }
      return [selectStmt({ table: o.table, columns: rcols(o.returning), where: o.where }, d), b];
    } };
  }
  function compile(d, items) {
    var out = { version: '2018-05-29', statements: [], variableMap: {}, variableTypeHintMap: {} }, n = 0, used = {};
    function paramName(hint) {
      if (hint === undefined) { return ':P' + n++; }
      var base = ':' + String(hint).toUpperCase(), k = base, i = 0;
      while (used[k]) { i++; k = base + '_' + i; }
      used[k] = true;
      return k;
    }
    items.forEach(function (it) {
      (it.__build ? it.__build(d) : [it]).forEach(function (s) {
        var text = s.strings[0];
        s.values.forEach(function (v, i) {
          var key = paramName(s.names ? s.names[i] : undefined);
          if (v && v.__hint) { out.variableTypeHintMap[key] = v.__hint; v = v.value; }
          out.variableMap[key] = v;
          text += key + s.strings[i + 1];
        });
        out.statements.push(text);
      });
    });
    return out;
  }
  function createPgStatement() { return compile('pg', Array.prototype.slice.call(arguments)); }
  function createMySQLStatement() { return compile('mysql', Array.prototype.slice.call(arguments)); }
  function toJsonObject(r) { return util.rds.toJsonObject(r); }
  return { sql: sql, select: select, insert: insert, update: update, remove: remove,
    createPgStatement: createPgStatement, createMySQLStatement: createMySQLStatement,
    toJsonObject: toJsonObject, typeHint: typeHint };
})(__util);
`
