/* @flow */
'use strict'

const parallel = require('async/parallel')
const rimraf = require('rimraf')

const MemoryStore = require('../src/memory')
const MountStore = require('../src/mount')
const LevelStore = require('../src/leveldb')
const FsStore = require('../src/fs')
const TieredStore = require('../src/tiered')

const Key = require('../src/key')
const utils = require('../src/utils')

/* :: import type {Callback} from '../src' */

let dir
let dir1
module.exports = [
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
  {name: 'Mount(Memory, Fs, Level)',
    create: () => {
      dir1 = utils.tmpdir()
      dir = utils.tmpdir()
      return new MountStore([{
        datastore: new LevelStore(dir1),
        prefix: new Key('/a')
      }, {
        datastore: new MemoryStore(),
        prefix: new Key('/z')
      }, {
        datastore: new FsStore(dir),
        prefix: new Key('/q')
      }])
    },
    close: (done/* : ?Callback<void> */) => {
      parallel([
        (cb) => rimraf(dir1, cb),
        (cb) => rimraf(dir, cb)
      ], done)
    }},
  {name: 'Leveldb',
    create: (d/* : ?string */) => {
      dir = d || utils.tmpdir()
      return new LevelStore(dir)
    },
    close: (done/* : ?Callback<void> */) => {
      rimraf(dir, done)
    }},
  {name: 'Leveldb(Memdown)',
    create: () => {
      return new LevelStore('', {db: require('memdown')})
    }},
  {name: 'Fs',
    create: (d/* : ?string */) => {
      dir = d || utils.tmpdir()
      return new FsStore(dir)
    },
    close: (done/* : ?Callback<void> */) => {
      rimraf(dir, done)
    }},
  {name: 'Flatfs',
    create: (d/* : ?string */) => {
      dir = d || utils.tmpdir()
      return new TieredStore([new FsStore(dir)])
    },
    close: (done/* : ?Callback<void> */) => {
      rimraf(dir, done)
    }},
  {name: 'Tired(Memory, Memory, Memory)',
    create: () => {
      return new TieredStore([new MemoryStore(), new MemoryStore(), new MemoryStore()])
    }},
  {name: 'Tired(Memory, Fs, Leveldb(Memdown))',
    create: () => {
      dir = utils.tmpdir()
      return new TieredStore([
        new MemoryStore(),
        new FsStore(dir),
        new LevelStore('', {db: require('memdown')})
      ])
    },
    close: (done/* : ?Callback<void> */) => {
      rimraf(dir, done)
    }}
]
