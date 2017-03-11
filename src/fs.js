/* @flow */
'use strict'

import type {Batch, Query, QueryResult, Callback} from './'

const fs = require('graceful-fs')
const pull = require('pull-stream')
const glob = require('pull-glob')
const setImmediate = require('async/setImmediate')
const series = require('async/series')
const each = require('async/each')
const mkdirp = require('mkdirp')
const writeFile = require('write-file-atomic')
const path = require('path')

const asyncFilter = require('./utils').asyncFilter
const asyncSort = require('./utils').asyncSort
const Key = require('./key')

export type FsInputOptions = {
  createIfMissing?: bool,
  errorIfExists?: bool,
  extension?: string
}

type FsOptions = {
  createIfMissing: bool,
  errorIfExists: bool,
  extension: string
}

/**
 * A datastore backed by the file system.
 *
 * Keys need to be sanitized before use, as they are written
 * to the file system as is.
 */
class FsDatastore {
  path: string
  opts: FsOptions

  constructor (location: string, opts: ?FsInputOptions) {
    this.path = path.resolve(location)
    this.opts = Object.assign({}, {
      createIfMissing: true,
      errorIfExists: false,
      extension: '.data'
    }, opts)

    if (this.opts.createIfMissing) {
      this._openOrCreate()
    } else {
      this._open()
    }
  }

  /**
   * Check if the path actually exists.
   * @private
   */
  _open () {
    if (!fs.existsSync(this.path)) {
      throw new Error(`Datastore directory: ${this.path} does not exist`)
    }

    if (this.opts.errorIfExists) {
      throw new Error(`Datastore directory: ${this.path} already exists`)
    }
  }

  /**
   * Create the directory to hold our data.
   *
   * @private
   */
  _create () {
    mkdirp.sync(this.path, {fs: fs})
  }

  /**
   * Tries to open, and creates if the open fails.
   *
   * @private
   */
  _openOrCreate () {
    try {
      this._open()
    } catch (err) {
      if (err.message.match('does not exist')) {
        this._create()
        return
      }

      throw err
    }
  }

  /**
   * Calculate the directory and file name for a given key.
   *
   * @private
   */
  _encode (key: Key): {dir: string, file: string} {
    const parent = key.parent().toString()
    const dir = path.join(this.path, parent)
    const name = key.toString().slice(parent.length)
    const file = path.join(dir, name + this.opts.extension)

    return {
      dir: dir,
      file: file
    }
  }

  /**
   * Calculate the original key, given the file name.
   *
   * @private
   */
  _decode (file: string): Key {
    const ext = this.opts.extension
    if (path.extname(file) !== ext) {
      throw new Error(`Invalid extension: ${path.extname(file)}`)
    }

    return new Key(file.slice(this.path.length, -ext.length))
  }

  put (key: Key, val: Buffer, callback: Callback<void>): void {
    const parts = this._encode(key)
    series([
      (cb) => mkdirp(parts.dir, {fs: fs}, cb),
      (cb) => writeFile(parts.file, val, cb)
    ], callback)
  }

  get (key: Key, callback: Callback<Buffer>): void {
    const parts = this._encode(key)
    fs.readFile(parts.file, callback)
  }

  has (key: Key, callback: Callback<bool>): void {
    const parts = this._encode(key)
    fs.access(parts.file, (err) => {
      callback(null, !err)
    })
  }

  delete (key: Key, callback: Callback<void>): void {
    const parts = this._encode(key)
    fs.unlink(parts.file, callback)
  }

  batch (): Batch<Buffer> {
    const puts = []
    const deletes = []
    return {
      put (key: Key, value: Buffer): void {
        puts.push({key: key, value: value})
      },
      delete (key: Key): void {
        deletes.push(key)
      },
      commit: (callback: (err: ?Error) => void) => {
        series([
          (cb) => each(puts, (p, cb) => {
            this.put(p.key, p.value, cb)
          }, cb),
          (cb) => each(deletes, (k, cb) => {
            this.delete(k, cb)
          }, cb)
        ], callback)
      }
    }
  }

  query (q: Query<Buffer>): QueryResult<Buffer> {
    let tasks = [
      glob(path.join(this.path, '**', '*' + this.opts.extension))
    ]

    if (!q.keysOnly) {
      tasks.push(pull.asyncMap((f, cb) => {
        fs.readFile(f, (err, buf) => {
          if (err) {
            return cb(err)
          }
          cb(null, {
            key: this._decode(f),
            value: buf
          })
        })
      }))
    } else {
      tasks.push(pull.map((f) => ({key: this._decode(f)})))
    }

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

    return pull.apply(null, tasks)
  }

  close (callback: (err: ?Error) => void): void {
    setImmediate(callback)
  }
}

module.exports = FsDatastore