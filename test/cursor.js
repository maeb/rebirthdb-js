const path = require('path')
const config = require('./config.js')
const rethinkdbdash = require(path.join(__dirname, '/../lib'))
const util = require(path.join(__dirname, '/util/common.js'))
const uuid = util.uuid
const assert = require('assert')

const iterall = require('iterall')
const {before, after, describe, it} = require('mocha')

describe('administration', () => {
  let r, connection, dbName, tableName, tableName2, cursor, result, feed

  const numDocs = 100 // Number of documents in the "big table" used to test the SUCCESS_PARTIAL
  const smallNumDocs = 5 // Number of documents in the "small table"

  before(async () => {
    r = await rethinkdbdash(config)

    dbName = uuid()
    tableName = uuid() // Big table to test partial sequence
    tableName2 = uuid() // small table to test success sequence

    // delete all but the system dbs
    for (let db of await r.dbList().run()) {
      if (db === 'rethinkdb' || db === 'test') {
        continue
      } else {
        try {
          await r.dbDrop(db).run()
        } catch (error) {
          assert.fail(error)
        }
      }
    }

    result = await r.dbCreate(dbName).run()
    assert.equal(result.dbs_created, 1)

    result = await r.db(dbName).tableCreate(tableName).run()
    assert.equal(result.tables_created, 1)

    result = await r.db(dbName).tableCreate(tableName2).run()
    assert.equal(result.tables_created, 1)
  })

  after(async () => {
    await r.getPoolMaster().drain()
  })

  it('Inserting batch - table 1', async () => {
    result = await r.db(dbName).table(tableName).insert(r.expr(Array(numDocs).fill({}))).run()
    assert.equal(result.inserted, numDocs)
  })

  it('Inserting batch - table 2', async () => {
    result = await r.db(dbName).table(tableName2).insert(r.expr(Array(smallNumDocs).fill({}))).run()
    assert.equal(result.inserted, smallNumDocs)
  })

  it('Updating batch', async () => {
    result = await r.db(dbName).table(tableName).update({
      date: r.now().sub(r.random().mul(1000000)),
      value: r.random()
    }, {nonAtomic: true}).run()
    assert.equal(result.replaced, 100)
  })

  it('`table` should return a cursor', async () => {
    cursor = await r.db(dbName).table(tableName).run({cursor: true})
    assert(cursor)
    assert.equal(cursor.toString(), '[object Cursor]')
  })

  it('`next` should return a document', async () => {
    result = await cursor.next()
    assert(result)
    assert(result.id)
  })

  it('`each` should work', async () => {
    cursor = await r.db(dbName).table(tableName).run({cursor: true})
    assert(cursor)

    await new Promise((resolve, reject) => {
      let count = 0
      cursor.each(function (err, result) {
        if (err) reject(err)
        count++
        if (count === numDocs) resolve()
      })
    })
  })

  it('`each` should work - onFinish - reach end', async () => {
    cursor = await r.db(dbName).table(tableName).run({cursor: true})
    assert(cursor)

    await new Promise((resolve, reject) => {
      let count = 0
      cursor.each(function (err, result) {
        if (err) reject(err)
        count++
      }, () => {
        if (count !== numDocs) {
          reject(new Error('expected count to equal numDocs', count, numDocs))
        }
        assert.equal(count, numDocs)
        resolve()
      })
    })
  })

  it('`each` should work - onFinish - return false', async () => {
    cursor = await r.db(dbName).table(tableName).run({cursor: true})
    assert(cursor)

    await new Promise((resolve, reject) => {
      let count = 0
      cursor.each(function (err, result) {
        if (err) reject(err)
        count++
        return false
      }, function () {
        count === 1 ? resolve() : reject(new Error('expected count to not equal 1'))
      })
    })
  })

  it('`eachAsync` should work', async () => {
    cursor = await r.db(dbName).table(tableName).run({cursor: true})
    assert(cursor)

    const history = []
    let count = 0
    let promisesWait = 0

    // TODO cleanup
    await cursor.eachAsync(async (result) => {
      history.push(count)
      count++
      await new Promise(function (resolve, reject) {
        setTimeout(function () {
          history.push(promisesWait)
          promisesWait--

          if (count === numDocs) {
            var expected = []
            for (var i = 0; i < numDocs; i++) {
              expected.push(i)
              expected.push(-1 * i)
            }
            assert.deepEqual(history, expected)
          }
          if (count > numDocs) {
            reject(new Error('eachAsync exceeded ' + numDocs + ' iterations'))
          } else {
            resolve()
          }
        }, 1)
      })
    })
  })

  it('`eachAsync` should work - callback style', async () => {
    cursor = await r.db(dbName).table(tableName).run({cursor: true})
    assert(cursor)

    let count = 0
    const now = Date.now()
    const timeout = 10

    await cursor.eachAsync(function (result, onRowFinished) {
      count++
      setTimeout(onRowFinished, timeout)
    })
    assert.equal(count, 100)
    const elapsed = Date.now() - now
    assert(elapsed >= timeout * count)
  })

  it('`toArray` should work', async () => {
    cursor = await r.db(dbName).table(tableName).run({cursor: true})
    result = await cursor.toArray()
    assert.equal(result.length, numDocs)
  })

  it('`toArray` should work - 2', async () => {
    cursor = await r.db(dbName).table(tableName2).run({cursor: true})
    result = await cursor.toArray()
    assert.equal(result.length, smallNumDocs)
  })

  it('`toArray` should work -- with a profile', async () => {
    cursor = await r.db(dbName).table(tableName).run({cursor: true, profile: true})
    result = await cursor.result.toArray()
    assert(Array.isArray(result))
    assert.equal(result.length, numDocs)
  })

  it('`toArray` should work with a datum', async () => {
    cursor = await r.expr([1, 2, 3]).run({cursor: true})
    result = await cursor.toArray()
    assert(Array.isArray(result))
    assert.deepEqual(result, [1, 2, 3])
  })

  it('`table` should return a cursor - 2', async () => {
    cursor = await r.db(dbName).table(tableName2).run({cursor: true})
    assert(cursor)
  })

  it('`next` should return a document - 2', async () => {
    result = await cursor.next()
    assert(result)
    assert(result.id)
  })

  it('`next` should work -- testing common pattern', async () => {
    cursor = await r.db(dbName).table(tableName2).run({cursor: true})
    assert(cursor)

    let i = 0
    try {
      while (true) {
        result = await cursor.next()
        assert(result)
        i++
      }
    } catch (e) {
      assert.equal(e.message, 'No more rows in the cursor.')
      assert.equal(smallNumDocs, i)
    }
  })

  it('`cursor.close` should return a promise', async () => {
    var cursor = await r.db(dbName).table(tableName2).run({cursor: true})
    await cursor.close()
  })

  it('`cursor.close` should still return a promise if the cursor was closed', async () => {
    cursor = await r.db(dbName).table(tableName2).changes().run()
    await cursor.close()
    result = cursor.close()
    try {
      result.then(() => {}) // Promise's contract is to have a `then` method
    } catch (e) {
      assert.fail(e)
    }
  })

  it('cursor should throw if the user try to serialize it in JSON', async () => {
    cursor = await r.db(dbName).table(tableName).run({cursor: true})

    try {
      cursor.toJSON()
    } catch (err) {
      assert.equal(err.message, 'You cannot serialize a Cursor to JSON. Retrieve data from the cursor with `toArray` or `next`.')
    }
  })

  it('Remove the field `val` in some docs - 1', async () => {
    result = await r.db(dbName).table(tableName).update({val: 1}).run()
    assert.equal(result.replaced, numDocs)

    result = await r.db(dbName).table(tableName)
      .orderBy({index: r.desc('id')})
      .sample(5).replace(r.row.without('val'))
      .run()
    assert.equal(result.replaced, 5)
  })

  it('Remove the field `val` in some docs - 2', async () => {
    result = await r.db(dbName).table(tableName).update({val: 1}).run()

    result = await r.db(dbName).table(tableName)
      .orderBy({index: r.desc('id')})
      .limit(5).replace(r.row.without('val'))
      .run()
    assert.equal(result.replaced, 5)
  })

  it('`toArray` with multiple batches - testing empty SUCCESS_COMPLETE', async () => {
    connection = await r.connect({host: config.host, port: config.port, authKey: config.authKey})
    assert(connection.open)

    cursor = await r.db(dbName).table(tableName).run(connection, {cursor: true, maxBatchRows: 1})
    assert(cursor)

    result = await cursor.toArray()
    assert(Array.isArray(result))
    assert.equal(result.length, 100)

    await connection.close()
    assert(!connection.open)
  })

  it('Automatic coercion from cursor to table with multiple batches', async () => {
    connection = await r.connect({host: config.host, port: config.port, authKey: config.authKey})
    assert(connection.open)

    result = await r.db(dbName).table(tableName).run(connection, {maxBatchRows: 1})
    assert(result.length > 0)

    await connection.close()
    assert(!connection.open)
  })

  it('`next` with multiple batches', async () => {
    connection = await r.connect({host: config.host, port: config.port, authKey: config.authKey})
    assert(connection.open)

    cursor = await r.db(dbName).table(tableName).run(connection, {cursor: true, maxBatchRows: 1})
    assert(cursor)

    let i = 0
    try {
      while (true) {
        result = await cursor.next()
        i++
      }
    } catch (e) {
      if ((i > 0) && (e.message === 'No more rows in the cursor.')) {
        await connection.close()
        assert(!connection.open)
      } else {
        assert.fail(e)
      }
    }
  })

  it('`next` should error when hitting an error -- not on the first batch', async () => {
    connection = await r.connect({host: config.host, port: config.port, authKey: config.authKey})
    assert(connection)

    cursor = await r.db(dbName).table(tableName)
      .orderBy({index: 'id'})
      .map(r.row('val').add(1))
      .run(connection, {cursor: true, maxBatchRows: 10})
    assert(cursor)

    let i = 0

    try {
      while (true) {
        result = await cursor.next()
        i++
      }
    } catch (e) {
      if ((i > 0) && (e.message.match(/^No attribute `val` in object/))) {
        await connection.close()
        assert(!connection.open)
      } else {
        assert.fail(e)
      }
    }
  })

  it('`changes` should return a feed', async () => {
    feed = await r.db(dbName).table(tableName).changes().run()
    assert(feed)
    assert.equal(feed.toString(), '[object Feed]')
    await feed.close()
  })

  it('`changes` should work with squash: true', async () => {
    feed = await r.db(dbName).table(tableName).changes({squash: true}).run()
    assert(feed)
    assert.equal(feed.toString(), '[object Feed]')
    await feed.close()
  })

  it('`get.changes` should return a feed', async () => {
    feed = await r.db(dbName).table(tableName).get(1).changes().run()
    assert(feed)
    assert.equal(feed.toString(), '[object AtomFeed]')
    await feed.close()
  })

  it('`orderBy.limit.changes` should return a feed', async () => {
    feed = await r.db(dbName).table(tableName).orderBy({index: 'id'}).limit(2).changes().run()
    assert(feed)
    assert.equal(feed.toString(), '[object OrderByLimitFeed]')
    await feed.close()
  })

  it('`changes` with `includeOffsets` should work', async () => {
    feed = await r.db(dbName).table(tableName).orderBy({index: 'id'}).limit(2).changes({
      includeOffsets: true,
      includeInitial: true
    }).run()

    let counter = 0

    const promise = new Promise((resolve, reject) => {
      feed.each(function (error, change) {
        if (error) reject(error)
        assert(typeof change.new_offset === 'number')
        if (counter >= 2) {
          assert(typeof change.old_offset === 'number')

          feed.close().then(resolve).error(reject)
        }
        counter++
      })
    })

    await r.db(dbName).table(tableName).insert({id: 0})
    await promise
  })

  it('`changes` with `includeTypes` should work', async () => {
    feed = await r.db(dbName).table(tableName).orderBy({index: 'id'}).limit(2).changes({
      includeTypes: true,
      includeInitial: true
    }).run()

    let counter = 0

    const promise = new Promise((resolve, reject) => {
      feed.each(function (error, change) {
        if (error) reject(error)
        assert(typeof change.type === 'string')
        if (counter > 0) {
          feed.close().then(resolve).error(reject)
        }
        counter++
      })
    })

    result = await r.db(dbName).table(tableName).insert({id: 0})
    assert.equal(result.errors, 1) // Duplicate primary key (depends on previous test case)
    await promise
  })

  it('`next` should work on a feed', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run()
    assert(feed)

    const promise = new Promise((resolve, reject) => {
      let i = 0
      while (true) {
        feed.next().then(assert)
        i++
        if (i === smallNumDocs) {
          return feed.close().then(resolve).error(reject)
        }
      }
    })

    await r.db(dbName).table(tableName2).update({foo: r.now()}).run()
    await promise
  })

  it('`next` should work on an atom feed', async () => {
    let idValue = uuid()
    feed = await r.db(dbName).table(tableName2).get(idValue).changes({includeInitial: true}).run()
    assert(feed)

    const promise = new Promise((resolve, reject) => {
      feed.next()
        .then((result) => assert.deepEqual(result, {new_val: null}))
        .then(() => feed.next())
        .then((result) => assert.deepEqual(result, {new_val: {id: idValue}, old_val: null}))
        .then(resolve)
        .catch(reject)
    })

    await r.db(dbName).table(tableName2).insert({id: idValue}).run()
    await promise
    await feed.close()
  })

  it('`close` should work on feed', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run()
    assert(feed)

    await feed.close()
  })

  it('`close` should work on feed with events', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run()

    const promise = new Promise((resolve, reject) => {
      feed.on('error', reject)
      feed.on('end', resolve)
    })

    await feed.close()
    await promise
  })

  it('`on` should work on feed', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run()
    assert(feed)

    const promise = new Promise((resolve, reject) => {
      let i = 0
      feed.on('data', function () {
        i++
        if (i === smallNumDocs) {
          feed.close().then(resolve).error(reject)
        }
      })
      feed.on('error', reject)
    })

    await r.db(dbName).table(tableName2).update({foo: r.now()}).run()
    await promise
  })

  it('`on` should work on cursor - a `end` event shoul be eventually emitted on a cursor', async () => {
    cursor = await r.db(dbName).table(tableName2).run({cursor: true})
    assert(cursor)

    const promise = new Promise((resolve, reject) => {
      cursor.on('end', resolve)
      cursor.on('error', reject)
    })

    await r.db(dbName).table(tableName2).update({foo: r.now()}).run()
    await promise
  })

  it('`next`, `each`, `toArray` should be deactivated if the EventEmitter interface is used', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run()

    feed.on('data', () => {})
    feed.on('error', assert.fail)

    try {
      feed.next()
      assert.fail('should throw')
    } catch (e) {
      assert(e.message === 'You cannot call `next` once you have bound listeners on the Feed.')
      await feed.close()
    }
  })

  it('Import with cursor as default', async () => {
    await util.sleep(2000)
    const r1 = rethinkdbdash({cursor: true, host: config.host, port: config.port, authKey: config.authKey, buffer: config.buffer, max: config.max, silent: true})

    cursor = await r1.db(dbName).table(tableName).run()
    assert.equal(cursor.toString(), '[object Cursor]')
    await cursor.close().then().catch(assert.fail)
  })

  it('`each` should not return an error if the feed is closed - 1', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run()
    assert(feed)

    const promise = new Promise((resolve, reject) => {
      let count = 0
      feed.each(function (err, result) {
        if (err) reject(err)
        if (result.new_val.foo instanceof Date) {
          count++
        }
        if (count === 1) {
          setTimeout(function () {
            feed.close().then(resolve).error(reject)
          }, 100)
        }
      })
    })

    await r.db(dbName).table(tableName2).limit(2).update({foo: r.now()}).run()
    await promise
  })

  it('`each` should not return an error if the feed is closed - 2', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run()
    assert(feed)

    const promise = new Promise((resolve, reject) => {
      let count = 0
      feed.each(function (err, result) {
        if (err) reject(err)
        if (result.new_val.foo instanceof Date) {
          count++
        }
        if (count === 2) {
          setTimeout(function () {
            feed.close().then(resolve).error(reject)
          }, 100)
        }
      })
    })
    await r.db(dbName).table(tableName2).limit(2).update({foo: r.now()}).run()
    await promise
  })

  it('events should not return an error if the feed is closed - 1', async () => {
    feed = await r.db(dbName).table(tableName2).get(1).changes().run()
    assert(feed)

    const promise = new Promise((resolve, reject) => {
      feed.each(function (err, result) {
        if (err) reject(err)
        if ((result.new_val != null) && (result.new_val.id === 1)) {
          feed.close().then(resolve).error(reject)
        }
      })
    })
    await r.db(dbName).table(tableName2).insert({id: 1}).run()
    await promise
  })

  it('events should not return an error if the feed is closed - 2', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run()
    assert(feed)

    const promise = new Promise((resolve, reject) => {
      let count = 0
      feed.on('data', function (result) {
        if (result.new_val.foo instanceof Date) {
          count++
        }
        if (count === 1) {
          setTimeout(function () {
            feed.close().then(resolve).error(reject)
          }, 100)
        }
      })
    })
    await r.db(dbName).table(tableName2).limit(2).update({foo: r.now()}).run()
    await promise
  })

  it('`includeStates` should work', async () => {
    feed = await r.db(dbName).table(tableName).orderBy({index: 'id'}).limit(10).changes({includeStates: true, includeInitial: true}).run()
    var i = 0

    await new Promise((resolve, reject) => {
      feed.each(function (err, change) {
        if (err) reject(err)
        i++
        if (i === 10) {
          feed.close().then(resolve).error(reject)
        }
      })
    })
  })

  it('`each` should return an error if the connection dies', async () => {
    connection = await r.connect({host: config.host, port: config.port, authKey: config.authKey})
    assert(connection)

    var feed = await r.db(dbName).table(tableName).changes().run(connection)

    feed.each(function (err, change) {
      assert(err.message.match(/^The connection was closed before the query could be completed for/))
    })
    // Kill the TCP connection
    connection.connection.end()
  })

  it('`eachAsync` should return an error if the connection dies', async () => {
    connection = await r.connect({host: config.host, port: config.port, authKey: config.authKey})
    assert(connection)

    var feed = await r.db(dbName).table(tableName).changes().run(connection)
    feed.eachAsync(function (change) {}).error(function (err) {
      assert(err.message.match(/^The connection was closed before the query could be completed for/))
    })
    // Kill the TCP connection
    connection.connection.end()
  })

  it('`asyncIterator` should return an async iterator', async () => {
    connection = await r.connect({host: config.host, port: config.port, authKey: config.authKey})
    assert(connection.open)

    const feed = await r.db(dbName).table(tableName).changes().run(connection)
    assert(feed)

    const iterator = feed.asyncIterator()
    assert(iterall.isAsyncIterable(iterator))
    // Kill the TCP connection
    connection.connection.end()
  })

  it('`asyncIterator` should have a working `next`method', async () => {
    feed = await r.db(dbName).table(tableName2).changes().run()
    assert(feed)

    const value = 1
    const iterator = feed.asyncIterator()
    assert(iterator)

    const promise = new Promise((resolve, reject) => {
      iterator.next().then(resolve).catch(reject)
    })

    await r.db(dbName).table(tableName2).update({foo: value}).run()
    result = await promise
    assert(result.value.new_val.foo === value)
  })
})
