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
  function builder() {
    var b = { strings: [''], values: [] };
    b.text = function (s) { b.strings[b.strings.length - 1] += s; return b; };
    b.bind = function (v) { b.values.push(v); b.strings.push(''); return b; };
    return b;
  }
  var binary = { eq: '=', ne: '<>', gt: '>', ge: '>=', lt: '<', le: '<=' };
  function list(b, field, kw, vals) {
    b.text(field + ' ' + kw + ' (');
    vals.forEach(function (x, i) { if (i) { b.text(', '); } b.bind(x); });
    b.text(')');
  }
  function cond(b, field, op, v) {
    if (binary[op]) { b.text(field + ' ' + binary[op] + ' ').bind(v); }
    else if (op === 'in') { list(b, field, 'IN', v); }
    else if (op === 'notIn') { list(b, field, 'NOT IN', v); }
    else if (op === 'between') { b.text(field + ' BETWEEN ').bind(v[0]).text(' AND ').bind(v[1]); }
    else if (op === 'beginsWith') { b.text(field + ' LIKE ').bind(v + '%'); }
    else if (op === 'contains') { b.text(field + ' LIKE ').bind('%' + v + '%'); }
    else if (op === 'notContains') { b.text(field + ' NOT LIKE ').bind('%' + v + '%'); }
    else if (op === 'attributeExists') { b.text(field + (v ? ' IS NOT NULL' : ' IS NULL')); }
    else { throw new Error('unsupported where operator: ' + op); }
  }
  function where(b, w) {
    Object.keys(w).forEach(function (k, n) {
      if (n) { b.text(' AND '); }
      var v = w[k];
      if (k === 'and' || k === 'or') {
        b.text('(');
        v.forEach(function (sub, i) {
          if (i) { b.text(k === 'and' ? ' AND ' : ' OR '); }
          b.text('('); where(b, sub); b.text(')');
        });
        b.text(')');
      } else if (k === 'not') {
        b.text('NOT ('); where(b, v); b.text(')');
      } else {
        Object.keys(v).forEach(function (op, i) { if (i) { b.text(' AND '); } cond(b, k, op, v[op]); });
      }
    });
  }
  function rcols(r) { return (r === true || r === '*') ? null : (typeof r === 'string' ? [r] : r); }
  function cols(c) { return c && c.length ? c.join(', ') : '*'; }
  function selectStmt(o) {
    var b = builder().text('SELECT ' + cols(o.columns) + ' FROM ' + o.table);
    if (o.where) { b.text(' WHERE '); where(b, o.where); }
    if (o.orderBy && o.orderBy.length) {
      b.text(' ORDER BY ' + o.orderBy.map(function (x) { return x.column + (x.dir ? ' ' + x.dir : ''); }).join(', '));
    }
    if (o.limit !== undefined) { b.text(' LIMIT ').bind(o.limit); }
    if (o.offset !== undefined) { b.text(' OFFSET ').bind(o.offset); }
    return b;
  }
  function eqWhere(values) {
    var w = {};
    Object.keys(values).forEach(function (k) { w[k] = { eq: values[k] }; });
    return w;
  }
  function select(o) { return { __build: function () { return [selectStmt(o)]; } }; }
  function insert(o) {
    return { __build: function (d) {
      var keys = Object.keys(o.values), b = builder();
      b.text('INSERT INTO ' + o.table + ' (' + keys.join(', ') + ') VALUES (');
      keys.forEach(function (k, i) { if (i) { b.text(', '); } b.bind(o.values[k]); });
      b.text(')');
      if (!o.returning) { return [b]; }
      if (d === 'pg') { return [b.text(' RETURNING ' + cols(rcols(o.returning)))]; }
      return [b, selectStmt({ table: o.table, columns: rcols(o.returning), where: eqWhere(o.values) })];
    } };
  }
  function update(o) {
    return { __build: function (d) {
      var keys = Object.keys(o.values), b = builder().text('UPDATE ' + o.table + ' SET ');
      keys.forEach(function (k, i) { if (i) { b.text(', '); } b.text(k + ' = ').bind(o.values[k]); });
      if (o.where) { b.text(' WHERE '); where(b, o.where); }
      if (!o.returning) { return [b]; }
      var rc = rcols(o.returning);
      if (d === 'pg') { return [b.text(' RETURNING ' + cols(rc))]; }
      return [b, selectStmt({ table: o.table, columns: rc, where: o.where })];
    } };
  }
  function remove(o) {
    return { __build: function (d) {
      var b = builder().text('DELETE FROM ' + o.table);
      if (o.where) { b.text(' WHERE '); where(b, o.where); }
      if (!o.returning) { return [b]; }
      var rc = rcols(o.returning);
      if (d === 'pg') { return [b.text(' RETURNING ' + cols(rc))]; }
      return [selectStmt({ table: o.table, columns: rc, where: o.where }), b];
    } };
  }
  function compile(d, items) {
    var out = { version: '2018-05-29', statements: [], variableMap: {}, variableTypeHintMap: {} }, n = 0;
    items.forEach(function (it) {
      (it.__build ? it.__build(d) : [it]).forEach(function (s) {
        var text = s.strings[0];
        s.values.forEach(function (v, i) {
          var key = ':P' + n++;
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
