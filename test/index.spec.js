/* @flow */
/* eslint-env mocha */
/* eslint max-nested-callbacks: ["error", 8] */
'use strict'

require('babel-register')

const pull = require('pull-stream')
const expect = require('chai').expect
const series = require('async/series')
const map = require('async/map')
const each = require('async/each')
const os = require('os')
const path = require('path')
const rimraf = require('rimraf')

const Key = require('../src/key')
const MemoryStore = require('../src/memory')
const MountStore = require('../src/mount')
const LevelStore = require('../src/leveldb')

const tmpDir = () => {
  return path.join(os.tmpdir(), `_test_dir_datastore-${Math.random().toString().replace('.', '')}`)
}

let dir
const stores = [
  ['Memory', () => new MemoryStore(), () => {}],
  ['Mount(Memory)', () => {
    return new MountStore([{
      datastore: new MemoryStore(),
      prefix: new Key('q')
    }, {
      datastore: new MemoryStore(),
      prefix: new Key('z')
    }])
  }, () => {}],
  ['Leveldb', () => {
    dir = tmpDir()
    return new LevelStore(dir)
  }, (done) => {
    rimraf(dir, done)
  }],
  ['Leveldb(Memdown)', () => {
    return new LevelStore('', {db: require('memdown')})
  }, () => {}]
]

describe('datastore', () => {
  stores.forEach((args) => describe(args[0], () => {
    const createStore = args[1]

    after(args[2])

    describe('put', () => {
      let store

      beforeEach(() => {
        store = createStore()
      })

      afterEach((done) => {
        store.close(done)
      })

      it('simple', (done) => {
        const k = new Key('/z/one')

        store.put(k, new Buffer('one'), done)
      })

      it('parallel', (done) => {
        const data = []
        for (let i = 0; i < 100; i++) {
          data.push([new Key(`/z/key${i}`), new Buffer(`data${i}`)])
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
        const k = new Key('/z/one')
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
        const k = new Key('/z/one')
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
          data.push([new Key(`/z/key${i}`), new Buffer(`data${i}`)])
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
          (cb) => store.put(new Key('/z/old'), new Buffer('old'), cb),
          (cb) => {
            b.put(new Key('/q/one'), new Buffer('1'))
            b.put(new Key('/q/two'), new Buffer('2'))
            b.put(new Key('/q/three'), new Buffer('3'))
            b.delete(new Key('/z/old'))
            b.commit(cb)
          },
          (cb) => map(
            ['/q/one', '/q/two', '/q/three', '/z/old'],
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
      const hello2 = {key: new Key('/z/hello2'), value: new Buffer('hello2')}
      const filter1 = (entry, cb) => {
        cb(null, !entry.key.toString().endsWith('hello'))
      }

      const filter2 = (entry, cb) => {
        cb(null, entry.key.toString().endsWith('hello2'))
      }

      const order1 = (res, cb) => {
        cb(null, res.sort((a, b) => {
          if (a.key.toString() < b.key.toString()) {
            return -1
          }
          return 1
        }))
      }

      const order2 = (res, cb) => {
        cb(null, res.sort((a, b) => {
          if (a.key.toString() < b.key.toString()) {
            return 1
          }
          return -1
        }))
      }

      const tests = [
        ['empty', {}, [hello, world, hello2]],
        ['prefix', {prefix: '/q'}, [hello]],
        ['1 filter', {filters: [filter1]}, [world, hello2]],
        ['2 filters', {filters: [filter1, filter2]}, [hello2]],
        ['limit', {limit: 1}, [hello]],
        ['offset', {offset: 1}, [world, hello2]],
        ['keysOnly', {keysOnly: true}, [{key: hello.key}, {key: world.key}, {key: hello2.key}]],
        ['1 order (1)', {orders: [order1]}, [hello, hello2, world]],
        ['1 order (reverse 1)', {orders: [order2]}, [world, hello2, hello]]
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
            if (t[1].orders == null) {
              const s = (a, b) => {
                if (a.key.toString() < b.key.toString()) {
                  return 1
                } else {
                  return -1
                }
              }
              expect(res.sort(s)).to.be.eql(t[2].sort(s))
            } else {
              expect(res).to.be.eql(t[2])
            }
            done()
          })
        )
      }))
    })
  }))
})
