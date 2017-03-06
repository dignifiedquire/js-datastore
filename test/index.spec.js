/* @flow */
/* eslint-env mocha */
'use strict'

require('babel-register')

const expect = require('chai').expect
const series = require('async/series')
const map = require('async/map')
const parallel = require('async/parallel')
const each = require('async/each')

const key = require('../src/key')

const stores = [
  ['Memory', require('../src/memory')]
]

describe('datastore', () => {
  stores.forEach((args) => describe(args[0], () => {
    const Store = args[1]

    describe('put', () => {
      it('simple', (done) => {
        const store = new Store()
        const k = key.create('one')

        store.put(k, new Buffer('one'), done)
      })
    })

    describe('get', () => {
      it('simple', (done) => {
        const store = new Store()
        const k = key.create('one')
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
      it('simple', (done) => {
        const store = new Store()
        const k = key.create('one')
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
    })

    describe('batch', () => {
      it('simple', (done) => {
        const store = new Store()
        const b = store.batch()

        series([
          (cb) => store.put('/old', new Buffer('old'), cb),
          (cb) => {
            b.put('/one', new Buffer('1'))
            b.put('/two', new Buffer('2')),
            b.put('/three', new Buffer('3'))
            b.delete('/old', cb)
            b.commit(cb)
          },
          (cb) => map(
            ['/one', '/two', '/three', '/old'],
            (k, cb) => store.has(k, cb),
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
      it('simple', (done) => {
        const store = new Store()

        const b = store.batch()
        const hello = {key: '/q/hello', value: new Buffer('hello')}
        const world = {key: '/z/world', value: new Buffer('world')}
        b.put(hello.key, hello.value)
        b.put(world.key, world.value)

        const tests = [
          [{},[hello, world]],
          [{prefix: '/q'}, [hello]],
          [{filters: [(res) => {
            cb(null, res[1])
          }]}, [world]]
          [{limit: 1}, [hello]],
          [{offset: 1}, [world]],
          [{keysOnly: true}, [{key: hello.key}, {key: world.key}]]
        ]

        series([
          (cb) => b.commit(cb),
          (cb) => each(tests, (t, cb) => {
            store.query(t[0], (err, res) => {
              expect(err).to.not.exist
              expect(res).to.be.eql(t[1])
              cb()
            })
          }, cb)
        ], done)
      })
    })
  }))
})
