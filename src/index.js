/* @flow */
'use strict'

import type {Key} from './key'

// -- Basics

export interface Datastore<Value> {
  put(Key, Value, (err: ?Error) => void): void;
  get(Key, (err: ?Error, val: ?Value) => void): void;
  has(Key, (err: ?Error, has: ?bool) => void): void;
  delete(Key, (err: ?Error) => void): void;

  query(Query<Value>, (err: ?Error, res: ?QueryResult<Value>) => void): void;

  batch(): Batch<Value>;
}

// -- Batch
export type Batch<Value> = {
  put(Key, Value): void,
  delete(Key): void,
  commit((err: ?Error) => void): void
}

// -- Query

export type Query<Value> = {
  prefix?: string,
  filters?: Array<Filter<Value>>,
  orders?: Array<Order<Value>>,
  limit?: number,
  offset?: number,
  keysOnly?: bool,
}

export type QueryResult<Value> = Array<QueryEntry<Value>>

export type QueryEntry<Value> = {
  key: Key,
  value: ?Value
}

export type Filter<Value> = (QueryEntry<Value>, (err: ?Error, truthy: ?bool) => void) => void

export type Order<Value> = (QueryResult<Value>, (err: ?Error, res: ?QueryResult<Value>) => void) => void
