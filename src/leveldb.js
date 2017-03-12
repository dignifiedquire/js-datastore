/* @flow */
'use strict'

/* :: import type {Callback, Batch, Query, QueryResult, QueryEntry} from './' */

const pull = require('pull-stream')
const levelup = require('levelup')

const asyncFilter = require('./utils').asyncFilter
const asyncSort = require('./utils').asyncSort
const Key = require('./key')

/**
 * A datastore backed by leveldb.
 */
/* :: export type LevelOptions = {
  createIfMissing?: bool,
  errorIfExists?: bool,
  compression?: bool,
  cacheSize?: number,
  db?: Object
} */
class LevelDatastore {
  /* :: db: levelup */

  constructor (path /* : string */, opts /* : ?LevelOptions */) {
    this.db = levelup(path, Object.assign({}, opts, {
      valueEncoding: 'binary'
    }))
  }

  put (key /* : Key */, value /* : Buffer */, callback /* : Callback<void> */) /* : void */ {
    this.db.put(key.toString(), value, callback)
  }

  get (key /* : Key */, callback /* : Callback<Buffer> */) /* : void */ {
    this.db.get(key.toString(), callback)
  }

  has (key /* : Key */, callback /* : Callback<bool> */) /* : void */ {
    this.db.get(key.toString(), (err, res) => {
      if (err) {
        if (err.notFound) {
          callback(null, false)
          return
        }
        callback(err)
        return
      }

      callback(null, true)
    })
  }

  delete (key /* : Key */, callback /* : Callback<void> */) /* : void */ {
    this.db.del(key.toString(), callback)
  }

  close (callback /* : Callback<void> */) /* : void */ {
    this.db.close(callback)
  }

  batch () /* : Batch<Buffer> */ {
    const ops = []
    return {
      put: (key /* : Key */, value /* : Buffer */) /* : void */ => {
        ops.push({
          type: 'put',
          key: key.toString(),
          value: value
        })
      },
      delete: (key /* : Key */) /* : void */ => {
        ops.push({
          type: 'del',
          key: key.toString()
        })
      },
      commit: (callback /* : Callback<void> */) /* : void */ => {
        this.db.batch(ops, callback)
      }
    }
  }

  query (q /* : Query<Buffer> */) /* : QueryResult<Buffer> */ {
    let values = true
    if (q.keysOnly != null) {
      values = !q.keysOnly
    }
    const iter = this.db.db.iterator({
      keys: true,
      values: values,
      keyAsBuffer: false
    })

    const rawStream = (end, cb) => {
      if (end) {
        iter.end(err => {
          cb(err || end)
        })
        return
      }

      iter.next((err, key, value) => {
        if (err) {
          cb(err)
          return
        }

        if (err == null && key == null && value == null) {
          cb(true)
          return
        }

        const res /* : QueryEntry<Buffer> */ = {
          key: new Key(key)
        }

        if (values) {
          res.value = new Buffer(value)
        }

        cb(null, res)
      })
    }

    let tasks = [rawStream]
    let filters = []

    if (q.prefix != null) {
      const prefix = q.prefix
      filters.push((e, cb) => cb(null, e.key.toString().startsWith(prefix)))
    }

    if (q.filters != null) {
      filters = filters.concat(q.filters)
    }

    tasks = tasks.concat(filters.map(f => asyncFilter(f)))

    if (q.orders != null) {
      tasks = tasks.concat(q.orders.map(o => asyncSort(o)))
    }

    if (q.offset != null) {
      let i = 0
      // $FlowFixMe
      tasks.push(pull.filter(() => i++ >= q.offset))
    }

    if (q.limit != null) {
      tasks.push(pull.take(q.limit))
    }

    return pull.apply(null, tasks)
  }
}

module.exports = LevelDatastore
