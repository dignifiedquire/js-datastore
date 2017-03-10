/* @flow */
'use strict'

import type Key from './key'
import type {Datastore, Batch, Query, QueryResult} from './'

const waterfall = require('async/waterfall')
const filter = require('async/filter')
const constant = require('async/constant')
const setImmedidate = require('async/setImmediate')

/**
 * Map one key onto another key.
 */
type KeyMapping = (Key) => Key

/**
 * An object with a pair of functions for (invertibly) transforming keys
 */
type KeyTransform = {
  convert: KeyMapping,
  invert: KeyMapping
}

/**
 * A datastore shim, that wraps around a given datastore, changing
 * the way keys look to the user, for example namespacing
 * keys, reversing them, etc.
 */
class KeyTransformDatastore<Value> {
  child: Datastore<Value>
  transform: KeyTransform

  constructor (child: Datastore<Value>, transform: KeyTransform) {
    this.child = child
    this.transform = transform
  }

  put (key: Key, val: Value, callback: (?Error) => void): void {
    this.child.put(this.transform.convert(key), val, callback)
  }

  get (key: Key, callback: (?Error, ?Value) => void): void {
    this.child.get(this.transform.convert(key), callback)
  }

  has (key: Key, callback: (?Error, ?bool) => void): void {
    this.child.has(this.transform.convert(key), callback)
  }

  delete (key: Key, callback: (?Error) => void): void {
    this.child.delete(this.transform.convert(key), callback)
  }

  batch (): Batch<Value> {
    const b = this.child.batch()
    return {
      put (key: Key, value: Value): void {
        b.put(this.transform.convert(key), value)
      },
      delete (key: Key): void {
        b.delete(this.transform.convert(key))
      },
      commit: (callback: (err: ?Error) => void) => {
        b.commit(callback)
      }
    }
  }

  query(q: Query<Value>, callback: (?Error, ?QueryResult<Value>) => void): void {
    this.child.query(q, (err, result) => {
      if (err) {
        return callback(err)
      }
      if (result == null) {
        return callback(err, result)
      }
      result.forEach((e) => {
        e.key = this.transform.invert(e.key)
      })
      callback(null, result)
    })
  }

  close (callback: (err: ?Error) => void): void {
    this.child.close(callback)
  }
}

module.exports = KeyTransformDatastore
