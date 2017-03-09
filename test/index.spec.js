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
      let store
      const hello = {key: '/q/hello', value: new Buffer('hello')}
      const world = {key: '/z/world', value: new Buffer('world')}
      const hello2 = {key: '/z/hello', value: new Buffer('hello2')}
      const filter1 = (entry, cb) => {
        cb(null, entry.key.startsWith('/z'))
      }

      const filter2 = (entry, cb) => {
        cb(null, entry.key.endsWith('hello'))
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
        store = new Store()

        const b = store.batch()

        b.put(hello.key, hello.value)
        b.put(world.key, world.value)
        b.put(hello2.key, hello2.value)

        b.commit(done)
      })

      tests.forEach((t) => it(t[0], (done) => {
        store.query(t[1], (err, res) => {
          expect(err).to.not.exist
          expect(res).to.be.eql(t[2])
          done()
        })
      }))
    })
  }))
})
