/* @flow */
'use strict'

import type {Key} from './key'
import type {Batch, Query, QueryResult} from './'

const setImmediate = require('async/setImmediate')

class MemoryDatastore {
  data: {[key: Key]: Buffer}

  constructor () {
    this.data = {}
  }

  put (key: Key, val: Buffer, callback: (?Error) => void): void {
    this.data[key] = val

    setImmediate(callback)
  }

  get (key: Key, callback: (?Error) => void): void {
    this.has(key, (err, exists) => {
      if (!exists) {
        return callback(new Error('No value'))
      }

      callback(null, this.data[key])
    })
  }

  has (key: Key, callback: (?Error, bool) => void): void {
    setImmediate(() => {
      callback(null, this.data[key] !== undefined)
    })
  }

  delete (key: Key, callback: (?Error) => void): void {
    delete this.data[key]

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
          this.data[v[0]] = v[1]
        })

        puts = []
        dels.forEach((key) => {
          delete this.data[key]
        })
        dels = []

        setImmediate(callback)
      }
    }
  }

  query(q: Query<Buffer>, callback: (?Error, ?QueryResult<Buffer>) => void): void {
    const keys = Object.keys(this.data)
    let res = keys.map((k) => ({
      key: k,
      value: this.data[k]
    }))

    if (q.prefix !== undefined) {
      res = res.filter((e) => e.key.startsWith(q.prefix))
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

    setImmediate(() => {
      callback(null, res)
    })
  }
}

module.exports = MemoryDatastore
