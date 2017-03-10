/* @flow */
'use strict'

import type {Batch, Query, QueryResult} from './'

const waterfall = require('async/waterfall')
const filter = require('async/filter')
const constant = require('async/constant')
const setImmedidate = require('async/setImmediate')

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

  query(q: Query<Buffer>, callback: (?Error, ?QueryResult<Buffer>) => void): void {
    const keys = Object.keys(this.data)
    let res = keys.map((k) => ({
      key: new Key(k),
      value: this.data[k]
    }))

    if (q.prefix != null) {
      const {prefix} = q
      res = res.filter((e) => e.key.toString().startsWith(prefix))
    }

    let tasks = [constant(res)]
    if (q.filters != null) {
      tasks = tasks.concat(q.filters.map((f) => {
        return (list, cb) => filter(list, f, cb)
      }))
    }

    if (q.orders != null) {
      tasks = tasks.concat(q.orders)
    }

    waterfall(tasks, (err, res) => {
      if (err) {
        return callback(err)
      }

      if (q.offset !== undefined || q.limit !== undefined) {
        res = res.slice(
          q.offset || 0,
          q.limit
        )
      }

      if (q.keysOnly === true) {
        res = res.map((v) => ({
          key: v.key
        }))
      }

      callback(null, res)
    })
  }

  close (callback: (err: ?Error) => void): void {
    setImmediate(callback)
  }
}

module.exports = MemoryDatastore
