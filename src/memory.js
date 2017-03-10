/* @flow */
'use strict'

import type {Batch, Query, QueryResult} from './'

const pull = require('pull-stream')
const waterfall = require('async/waterfall')
const filter = require('async/filter')
const constant = require('async/constant')
const setImmedidate = require('async/setImmediate')

const asyncFilter = require('./utils').asyncFilter
const asyncSort = require('./utils').asyncSort
const Key = require('./key')

class MemoryDatastore {
  data: {[key: string]: Buffer}

  constructor () {
    this.data = {}
  }

  put (key: Key, val: Buffer, callback: (?Error) => void): void {
    this.data[key.toString()] = val

    setImmediate(callback)
  }

  get (key: Key, callback: (?Error, ?Buffer) => void): void {
    this.has(key, (err, exists) => {
      if (!exists) {
        return callback(new Error('No value'))
      }

      callback(null, this.data[key.toString()])
    })
  }

  has (key: Key, callback: (?Error, bool) => void): void {
    setImmediate(() => {
      callback(null, this.data[key.toString()] !== undefined)
    })
  }

  delete (key: Key, callback: (?Error) => void): void {
    delete this.data[key.toString()]

    setImmediate(() => {
      callback()
    })
  }

  batch (): Batch<Buffer> {
    let puts = []
    let dels = []

    return {
      put (key: Key, value: Buffer): void {
        puts.push([key, value])
      },
      delete (key: Key): void {
        dels.push(key)
      },
      commit: (callback: (err: ?Error) => void) => {
        puts.forEach((v) => {
          this.data[v[0].toString()] = v[1]
        })

        puts = []
        dels.forEach((key) => {
          delete this.data[key.toString()]
        })
        dels = []

        setImmediate(callback)
      }
    }
  }

  query(q: Query<Buffer>): QueryResult<Buffer> {
    let tasks = [
      pull.keys(this.data),
      pull.map((k) => ({
        key: new Key(k),
        value: this.data[k]
      }))
    ]

    let filters = []

    if (q.prefix != null) {
      const {prefix} = q
      filters.push((e, cb) => cb(null, e.key.toString().startsWith(prefix)))
    }

    if (q.filters != null) {
      filters = filters.concat(q.filters)
    }

    tasks = tasks.concat(filters.map((f) => asyncFilter(f)))

    if (q.orders != null) {
      tasks = tasks.concat(q.orders.map((o) => asyncSort(o)))
    }

    if (q.offset != null) {
      let i = 0
      // $FlowFixMe
      tasks.push(pull.filter(() => i++ >= q.offset))
    }

    if (q.limit != null) {
      tasks.push(pull.take(q.limit))
    }

    if (q.keysOnly === true) {
      tasks.push(pull.map((e) => ({key: e.key})))
    }

    return pull.apply(null, tasks)
  }

  close (callback: (err: ?Error) => void): void {
    setImmediate(callback)
  }
}

module.exports = MemoryDatastore
