/* @flow */
'use strict'

const path = require('path')

export type Key = string

function create (s: string): Key {
  return clean(s)
}

function clean (s: string): Key {
  if (!s) {
    return '/'
  }

  if (s[0] !== '/') {
    s = '/' + s
  }

  return path.normalize(s)
}

module.exports = {
  create,
  clean
}
