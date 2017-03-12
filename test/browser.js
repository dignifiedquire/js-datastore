/* @flow */
'use strict'

const test = require('./index-test')

const uuid = require('uuid/v4')
const memdown = require('memdown')
const leveljs = require('level-js')

const MemoryStore = require('../src/memory')
const MountStore = require('../src/mount')
const LevelStore = require('../src/leveldb')
const TieredStore = require('../src/tiered')

const Key = require('../src/key')
const utils = require('../src/utils')

let dir
const stores = [
  {name: 'Memory', create: () => new MemoryStore()},
  {name: 'Mount(Memory)',
    create: () => {
      return new MountStore([{
        datastore: new MemoryStore(),
        prefix: new Key('/q')
      }, {
        datastore: new MemoryStore(),
        prefix: new Key('/a')
      }, {
        datastore: new MemoryStore(),
        prefix: new Key('/z')
      }])
    }},
  {name: 'Mount(Memory, Level(Memdown), Level)',
    create: () => {
      dir = uuid()
      return new MountStore([{
        datastore: new LevelStore(dir, {db: leveljs}),
        prefix: new Key('/a')
      }, {
        datastore: new LevelStore(dir, {db: memdown}),
        prefix: new Key('/q')
      }, {
        datastore: new MemoryStore(),
        prefix: new Key('/z')
      }])
    },
    close: (done) => {
      leveljs.destroy(dir, done)
    }},
  {name: 'Leveldb',
    create: () => {
      dir = uuid()
      return new LevelStore(dir, {db: leveljs})
    },
    close: (done) => {
      leveljs.destroy(dir, done)
    }},
  {name: 'Leveldb(Memdown)',
    create: () => {
      return new LevelStore('', {db: memdown})
    }},
  {name: 'Tired(Memory, Memory, Memory)',
    create: () => {
      return new TieredStore([new MemoryStore(), new MemoryStore(), new MemoryStore()])
    }},
  {name: 'Tired(Memory, Leveldb(Memdown))',
    create: () => {
      dir = utils.tmpdir()
      return new TieredStore([
        new MemoryStore(),
        new LevelStore('', {db: memdown})
      ])
    }}
]

test(stores)
