/* @flow */
/* eslint-env mocha */
'use strict'

require('babel-register')

const pull = require('pull-stream')
const expect = require('chai').expect
const series = require('async/series')
const map = require('async/map')
const each = require('async/each')

const Key = require('../src/key')
const MemoryStore = require('../src/memory')

const stores = [
  ['Memory', () => new MemoryStore()]
]

describe('datastore', () => {
  stores.forEach((args) => describe(args[0], () => {
    const createStore = args[1]

    describe('put', () => {
      let store

      beforeEach(() => {
        store = createStore()
      })

      afterEach((done) => {
        store.close(done)
      })

      it('simple', (done) => {
        const k = new Key('one')

        store.put(k, new Buffer('one'), done)
      })

      it('parallel', (done) => {
        const data = []
        for (let i = 0; i < 100; i++) {
          data.push([new Key(`key${i}`), new Buffer(`data${i}`)])
        }

        each(data, (d, cb) => {
          store.put(d[0], d[1], cb)
        }, (err) => {
          expect(err).to.not.exist
          map(data, (d, cb) => {
            store.get(d[0], cb)
          }, (err, res) => {
            expect(err).to.not.exist
            res.forEach((res, i) => {
              expect(res).to.be.eql(data[i][1])
            })
            done()
          })
        })
      })
    })

    describe('get', () => {
      let store

      beforeEach(() => {
        store = createStore()
      })

      afterEach((done) => {
        store.close(done)
      })

      it('simple', (done) => {
        const k = new Key('one')
        series([
          (cb) => store.put(k, new Buffer('hello'), cb),
          (cb) => store.get(k, (err, res) => {
            expect(err).to.not.exist
            expect(res).to.be.eql(new Buffer('hello'))
            cb()
          })
        ], done)
      })
    })

    describe('delete', () => {
      let store

      beforeEach(() => {
        store = createStore()
      })

      afterEach((done) => {
        store.close(done)
      })

      it('simple', (done) => {
        const k = new Key('one')
        series([
          (cb) => store.put(k, new Buffer('hello'), cb),
          (cb) => store.get(k, (err, res) => {
            expect(err).to.not.exist
            expect(res).to.be.eql(new Buffer('hello'))
            cb()
          }),
          (cb) => store.delete(k, cb),
          (cb) => store.has(k, (err, exists) => {
            expect(err).to.not.exist
            expect(exists).to.be.eql(false)
            cb()
          })
        ], done)
      })

      it('parallel', (done) => {
        const data = []
        for (let i = 0; i < 100; i++) {
          data.push([new Key(`key${i}`), new Buffer(`data${i}`)])
        }

        series([
          (cb) => each(data, (d, cb) => {
            store.put(d[0], d[1], cb)
          }, cb),
          (cb) => map(data, (d, cb) => {
            store.has(d[0], cb)
          }, (err, res) => {
            expect(err).to.not.exist
            res.forEach((res, i) => {
              expect(res).to.be.eql(true)
            })
            cb()
          }),
          (cb) => each(data, (d, cb) => {
            store.delete(d[0], cb)
          }, cb),
          (cb) => map(data, (d, cb) => {
            store.has(d[0], cb)
          }, (err, res) => {
            expect(err).to.not.exist
            res.forEach((res, i) => {
              expect(res).to.be.eql(false)
            })
            cb()
          })
        ], done)
      })
    })

    describe('batch', () => {
      let store

      beforeEach(() => {
        store = createStore()
      })

      afterEach((done) => {
        store.close(done)
      })

      it('simple', (done) => {
        const b = store.batch()

        series([
          (cb) => store.put(new Key('/old'), new Buffer('old'), cb),
          (cb) => {
            b.put(new Key('/one'), new Buffer('1'))
            b.put(new Key('/two'), new Buffer('2'))
            b.put(new Key('/three'), new Buffer('3'))
            b.delete(new Key('/old'), cb)
            b.commit(cb)
          },
          (cb) => map(
            ['/one', '/two', '/three', '/old'],
            (k, cb) => store.has(new Key(k), cb),
            (err, res) => {
              expect(err).to.not.exist
              expect(res).to.be.eql([true, true, true, false])
              cb()
            }
          )
        ], done)
      })
    })

    describe('query', () => {
      let store
      const hello = {key: new Key('/q/hello'), value: new Buffer('hello')}
      const world = {key: new Key('/z/world'), value: new Buffer('world')}
      const hello2 = {key: new Key('/z/hello'), value: new Buffer('hello2')}
      const filter1 = (entry, cb) => {
        cb(null, entry.key.toString().startsWith('/z'))
      }

      const filter2 = (entry, cb) => {
        cb(null, entry.key.toString().endsWith('hello'))
      }

      const orderIdentity = (res, cb) => {
        cb(null, res)
      }

      const orderReverse = (res, cb) => {
        cb(null, res.reverse())
      }

      const tests = [
        ['empty', {}, [hello, world, hello2]],
        ['prefix', {prefix: '/q'}, [hello]],
        ['1 filter', {filters: [filter1]}, [world, hello2]],
        ['2 filters', {filters: [filter1, filter2]}, [hello2]],
        ['limit', {limit: 1}, [hello]],
        ['offset', {offset: 1}, [world, hello2]],
        ['keysOnly', {keysOnly: true}, [{key: hello.key}, {key: world.key}, {key: hello2.key}]],
        ['1 order (identity)', {orders: [orderIdentity]}, [hello, world, hello2]],
        ['1 order (reverse)', {orders: [orderReverse]}, [hello2, world, hello]]
      ]

      before((done) => {
        store = createStore()

        const b = store.batch()

        b.put(hello.key, hello.value)
        b.put(world.key, world.value)
        b.put(hello2.key, hello2.value)

        b.commit(done)
      })

      after((done) => {
        store.close(done)
      })

      tests.forEach((t) => it(t[0], (done) => {
        pull(
          store.query(t[1]),
          pull.collect((err, res) => {
            expect(err).to.not.exist
            expect(res).to.be.eql(t[2])
            done()
          })
        )
      }))
    })
  }))
})
