const mongoose = require('mongoose');
const { Leader } = require('mongo-leader');
const CollectionManager = require('./lib/collection-manager');

function deferred () {
  let _resolve;
  let _reject;

  const promise = new Promise((resolve, reject) => {
    _resolve = resolve;
    _reject = reject;
  });

  return {
    resolve: _resolve,
    reject: _reject,
    promise,
  };
}

function delay (ms) {
  const done = deferred();
  setTimeout(done.resolve, ms);

  return done.promise;
}

async function onChange (data) {
  await delay(1000);
  console.log(`${data.operationType}: `, data);
}

async function onError (error) {
  console.error('Error from stream', error);
}

async function main () {
  const isReady = deferred();
  mongoose.connect('mongodb://localhost:27017/?replicaSet=rs1');
  mongoose.connection
    .on('connected', () => {
      console.log('DB is connected');
      isReady.resolve();
    })
    .on('disconnected', () => {
      console.log('DB is disconnected');
    })
    .on('error', (error) => {
      console.log('DB error: ', error);
      isReady.reject(error);
    })
    .on('close', () => {
      console.log('DB is closed');
    });

  await isReady.promise;

  const leader = new Leader(mongoose.connection.useDb('pud', { useCache: true }).db, { ttl: 5000, wait: 1000 });
  const collections = [];
  ['b', 'c'].forEach(async (col) => {
    const collectionManager = new CollectionManager(mongoose.connection.useDb('pud', { useCache: true }), {
      watch: {
        fullDocument: 'updateLookup',
        readPreference: 'secondaryPreferred'
      },
      collection: col,
      handlers: {
        onChange,
        onError,
        onClose: () => { console.log('stream is closed'); },
        onEnd: () => { console.log('stream is ended'); }
      },
      logger: console,
    });
    collections.push(collectionManager);
  });

  leader
    .on('elected', async () => {
      console.log('I am a leader');
      await Promise.all(collections.map(c => c.watch()));
    })
    .on('revoked', async () => {
      console.log('I am stand by');
      await Promise.all(collections.map(c => c.close()));
    });

  process.on('SIGINT', async () => {
    await Promise.all(collections.map(c => c.close()));
    await mongoose.connection.close();

    console.log('everything is closed. Bye');
    process.exit(0);
  });
}

main().catch(console.error);
