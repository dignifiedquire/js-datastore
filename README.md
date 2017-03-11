# JavaScript Datastore

[![Build Status](https://travis-ci.org/dignifiedquire/js-datastore.svg?branch=master)](https://travis-ci.org/dignifiedquire/js-datastore)

> Implementation of the datastore interface in JavaScript

## Implementations

- Backed Implementations
  - Memory: [`src/memory`](src/memory.js)
  - leveldb: [`src/leveldb`](src/leveldb.js) (supports any levelup compatible backend)
  - File System: [`src/fs`](src/fs.js)
- Wrapper Implementations
  - Mount: [`src/mount`](src/mount.js)
  - Keytransform: [`src/keytransform`](src/keytransform.js)
  - Sharding: [`src/sharding`](src/sharding.js)

If you want the same functionality as [go-ds-flatfs](https://github.com/ipfs/go-ds-flatfs), use sharding with fs.

```js
const FsStore = require('./src/fs')
const ShardingStore = require('./src/sharding')
const NextToLast = require('./src/shard').NextToLast

const fs = new FsStore('path/to/store')
ShardingStore.createOrOpen(fs, new NextToLast(2), (err, flatfs) => {
  // flatfs now works like go-flatfs
})
```

## API

The exact types can be found in [`src/index.js`](src/index.js).

### `put(Key, Value, (err: ?Error) => void): void`

### `get(Key, (err: ?Error, val: ?Value) => void): void`

### `delete(Key, (err: ?Error) => void): void`

### `query(Query<Value>): QueryResult<Value>)`

#### `Query`

Object in the form with the following optional properties

- `prefix?: string`
- `filters?: Array<Filter<Value>>`
- `orders?: Array<Order<Value>>`
- `limit?: number`
- `offset?: number`
- `keysOnly?: bool`

### batch(): Batch<Value>

#### `put(Key, Value): void`
#### `delete(Key): void`
#### `commit((err: ?Error) => void): void`

### `close((err: ?Error) => void): void`

Close the datastore, this should always be called to ensure resources are cleaned up.

## License

MIT 2017 Protocol Labs
