/* @flow */
/* eslint-env mocha */
'use strict'

require('babel-register')
const path = require('path')
const expect = require('chai').expect
const mkdirp = require('mkdirp')

const FsStore = require('../src/fs')
const Key = require('../src/key')
const utils = require('../src/utils')

describe('FsDatastore', () => {
  describe('construction', () => {
    it('defaults - folder missing', () => {
      const dir = utils.tmpdir()
      expect(
        () => new FsStore(dir)
      ).to.not.throw()
    })

    it('defaults - folder exists', () => {
      const dir = utils.tmpdir()
      mkdirp.sync(dir)
      expect(
        () => new FsStore(dir)
      ).to.not.throw()
    })

    it('createIfMissing: false - folder missing', () => {
      const dir = utils.tmpdir()
      expect(
        () => new FsStore(dir, {createIfMissing: false})
      ).to.throw()
    })

    it('errorIfExists: true - folder exists', () => {
      const dir = utils.tmpdir()
      mkdirp.sync(dir)
      expect(
        () => new FsStore(dir, {errorIfExists: true})
      ).to.throw()
    })
  })

  it('_encode and _decode', () => {
    const dir = utils.tmpdir()
    const fs = new FsStore(dir)

    expect(
      fs._encode(new Key('hello/world'))
    ).to.eql({
      dir: path.join(dir, 'hello'),
      file: path.join(dir, 'hello', 'world.data')
    })

    expect(
      fs._decode(fs._encode(new Key('hello/world/test:other')).file)
    ).to.eql(
      new Key('hello/world/test:other')
    )
  })
})
