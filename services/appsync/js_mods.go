package appsync

// jsPrelude defines the @aws-appsync/utils module objects. It runs before the
// user module; __util, __runtime and __extensions are bound from Go.
const jsPrelude = `
var __mod_utils = { util: __util, runtime: __runtime, extensions: __extensions };
var __mod_ddb = (function (util) {
  var tm = util.dynamodb.toMapValues;
  function clean(o) { Object.keys(o).forEach(function (k) { if (o[k] === undefined) { delete o[k]; } }); return o; }
  function expr(c) { return util.transform.toDynamoDBConditionExpression(c); }
  function op(kind, value, extra) {
    var r = { __op: kind, value: value };
    if (extra !== undefined) { r.index = extra; }
    return r;
  }
  var operations = {
    add: function (v) { return op('add', v); },
    remove: function () { return op('remove'); },
    replace: function (v) { return op('replace', v); },
    increment: function (v) { return op('increment', v); },
    decrement: function (v) { return op('decrement', v); },
    append: function (v) { return op('append', v); },
    prepend: function (v) { return op('prepend', v); },
    updateListItem: function (v, i) { return op('updateListItem', v, i); }
  };
  function get(o) { return clean({ operation: 'GetItem', key: tm(o.key), consistentRead: o.consistentRead }); }
  function put(o) {
    return clean({ operation: 'PutItem', key: tm(o.key), attributeValues: tm(o.item),
      condition: o.condition ? expr(o.condition) : undefined });
  }
  function remove(o) {
    return clean({ operation: 'DeleteItem', key: tm(o.key), condition: o.condition ? expr(o.condition) : undefined });
  }
  function scan(o) {
    o = o || {};
    return clean({ operation: 'Scan', index: o.index, limit: o.limit, consistentRead: o.consistentRead,
      nextToken: o.nextToken, totalSegments: o.totalSegments, segment: o.segment, select: o.select,
      filter: o.filter ? expr(o.filter) : undefined });
  }
  function sync(o) {
    o = o || {};
    return clean({ operation: 'Sync', basePartitionKey: o.basePartitionKey, deltaIndexName: o.deltaIndexName,
      limit: o.limit, nextToken: o.nextToken, lastSync: o.lastSync,
      filter: o.filter ? expr(o.filter) : undefined });
  }
  function query(o) {
    return clean({ operation: 'Query', query: expr(o.query), index: o.index, nextToken: o.nextToken, limit: o.limit,
      scanIndexForward: o.scanIndexForward, consistentRead: o.consistentRead, select: o.select,
      filter: o.filter ? expr(o.filter) : undefined });
  }
  function update(o) {
    var sets = [], removes = [], adds = [], names = {}, values = {}, n = 0;
    Object.keys(o.update).forEach(function (field) {
      var v = o.update[field], nk = '#f' + n, vk = ':v' + n; n++;
      names[nk] = field;
      var kind = (v && v.__op) || 'replace', val = (v && v.__op) ? v.value : v;
      if (kind === 'remove') { removes.push(nk); return; }
      if (kind === 'add') { adds.push(nk + ' ' + vk); values[vk] = util.dynamodb.toDynamoDB(val); return; }
      values[vk] = util.dynamodb.toDynamoDB(val);
      if (kind === 'increment') { sets.push(nk + ' = ' + nk + ' + ' + vk); }
      else if (kind === 'decrement') { sets.push(nk + ' = ' + nk + ' - ' + vk); }
      else if (kind === 'append') { sets.push(nk + ' = list_append(' + nk + ', ' + vk + ')'); }
      else if (kind === 'prepend') { sets.push(nk + ' = list_append(' + vk + ', ' + nk + ')'); }
      else if (kind === 'updateListItem') { sets.push(nk + '[' + v.index + '] = ' + vk); }
      else { sets.push(nk + ' = ' + vk); }
    });
    var parts = [];
    if (sets.length) { parts.push('SET ' + sets.join(', ')); }
    if (removes.length) { parts.push('REMOVE ' + removes.join(', ')); }
    if (adds.length) { parts.push('ADD ' + adds.join(', ')); }
    var r = { operation: 'UpdateItem', key: tm(o.key),
      update: { expression: parts.join(' '), expressionNames: names, expressionValues: values } };
    if (o.condition) { r.condition = expr(o.condition); }
    return clean(r);
  }
  return { get: get, put: put, remove: remove, scan: scan, sync: sync, query: query, update: update, operations: operations };
})(__util);
`
