import { ObjectId } from 'mongodb';
import type {
  Collection as MongoCollection,
  Document,
  WithId,
  InsertOneOptions,
  BulkWriteOptions,
  DeleteOptions,
  UpdateResult,
  AggregateOptions,
  IndexSpecification,
  CreateIndexesOptions,
} from 'mongodb';

import Cursor from './cursor';
import Bulk from './bulk';
import type Database from './database';

// TODO: Make this configurable by users
const writeOpts = { writeConcern: { w: 1 }, ordered: true }

export default class Collection {
  private db: Database;
  private name: string;

  constructor(db: Database, name: string) {
    this.db = db;
    this.name = name;
  }

  connect(): Promise<MongoCollection | undefined> {
    return this.db.connect().then(connection => connection?.collection(this.name));
  }

  find(query, projection, opts): Promise<[] | WithId<Document>[] | undefined> {
    return this
      .connect()
      .then(collection => {
        const options = Object.assign({ projection }, opts);
        return collection?.find(query, options).toArray();
      });
  }

  findAsCursor(query, projection, opts): Cursor {
    return new Cursor(() => this
      .connect()
      .then(collection => {
        const options = Object.assign({ projection }, opts);
        return collection?.find(query, options);
      }));
  }

  findOne(query, projection, opts) {
    return this
      .findAsCursor(query, projection, opts)
      .next();
  }

  findAndModify(opts) {
    return this.runCommand('findAndModify', opts)
      .then(result => result?.value);
  }

  count(query) {
    return this
      .connect()
      .then(collection => collection?.countDocuments(query));
  }

  distinct(field, query) {
    return this
      .runCommand('distinct', { key: field, query: query })
      .then(result => result?.values);
  }

  insert(docOrDocs, opts) {
    return Array.isArray(docOrDocs)
      ? this.insertMany(docOrDocs, opts)
      : this.insertOne(docOrDocs, opts);
  }

  insertOne<T extends { _id?: ObjectId }>(doc: T, opts: InsertOneOptions): Promise<T> {
    if (!doc._id) doc._id = new ObjectId();

    return this
      .connect()
      .then(collection => collection?.insertOne(doc, Object.assign({}, writeOpts, opts)))
      .then(() => doc);
  }

  insertMany<T extends { _id?: ObjectId }>(docs: T[], opts: BulkWriteOptions): Promise<T[]> {
    for (let i = 0; i < docs.length; i++) {
      if (!docs[i]._id) docs[i]._id = new ObjectId();
    }

    return this
      .connect()
      .then(collection => collection?.insertMany(docs, Object.assign({}, writeOpts, opts)))
      .then(() => docs);
  }

  update(query, update, opts): Promise<UpdateResult & { ok: boolean, n: number }> {
    opts = opts || {};
    const isMulti = opts.multi

    return this
      .connect()
      .then(collection => {
        if (isMulti) {
          return collection?.updateMany(query, update, { ...writeOpts, ...opts });
        } else {
          return collection?.updateOne(query, update, { ...writeOpts, ...opts });
        }
      })
      .then((result: UpdateResult | undefined) => {
        // Parse the result to be compliant with the old standard:
        // https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#write-results
        // Avoid using "ok" and "n".
        return {
          ...result,
          ok: result?.acknowledged,
          n: result?.modifiedCount || result?.upsertedCount,
        }
      });
  }

  replaceOne(filter, replacement, opts) {
    opts = opts || {};

    return this
      .connect()
      .then(collection => collection?.replaceOne(filter, replacement, { ...writeOpts, ...opts }));
  }

  save(doc, opts) {
    opts = opts || {};

    if (doc._id) {
      return this
        .update({ _id: doc._id }, { $set: doc }, { upsert: true, ...opts})
        .then(() => doc);
    } else {
      return this.insert(doc, opts);
    }
  }

  remove(query, opts: DeleteOptions & { justOne?: boolean }) {
    opts = opts || { justOne: false };

    if (typeof opts == 'boolean') {
      opts = { justOne: opts };
    }

    return this.connect()
      .then(collection => collection?.[opts.justOne ? 'deleteOne' : 'deleteMany']?.(query, Object.assign({}, writeOpts, opts)))
      .then(result => {
        // Parse the result to be compliant with the old standard:
        // https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst#write-results
        // Avoid using "ok" and "n".
        return {
          ...result,
          ok: result?.acknowledged,
          n: result?.deletedCount
        }
      });
  }

  rename(name, opts) {
    return this
      .connect()
      .then(collection => collection.rename(name, opts));
  }

  drop() {
    return this.runCommand('drop');
  }

  stats() {
    return this.runCommand('collStats');
  }

  /** @deprecated Please use the aggregation pipeline instead */
  mapReduce(map, reduce, opts) {
    opts = opts || {};

    return this
      .connect()
      .then(collection => collection.mapReduce(map, reduce, opts));
  }

  runCommand(cmd, opts?: object) {
    opts = opts || {}

    return this.db
      .connect()
      .then(collection => collection?.command({
        [cmd]: this.name,
        ...opts,
      }));
  }

  toString() {
    return this.name;
  }

  dropIndexes(): Promise<Document | undefined>  {
    return this.runCommand('dropIndexes', { index: '*' });
  }

  dropIndex(index): Promise<Document | undefined> {
    return this.runCommand('dropIndexes', { index });
  }

  createIndex(index: IndexSpecification, opts: CreateIndexesOptions = {}): Promise<void | string>  {
    return this.connect()
      .then(collection => collection?.createIndex(index, opts));
  }

  ensureIndex(index: IndexSpecification, opts: CreateIndexesOptions = {}): Promise<void | string>  {
    return this.createIndex(index, opts)
  }

  getIndexes(): Promise<Document[] | undefined>  {
    return this.connect()
      .then(collection => collection?.indexes());
  }

  reIndex(): Promise<Document | undefined>  {
    return this.runCommand('reIndex');
  }

  isCapped(): Promise<boolean>  {
    return this.connect()
      .then(collection => collection?.isCapped())
      .then(isCapped => !!isCapped);
  }

  aggregate(pipeline: Document[], opts: AggregateOptions = {}): Promise<any[]> {
    return this.aggregateAsCursor(pipeline, opts).toArray();
  }

  aggregateAsCursor(pipeline: Document[], opts: AggregateOptions = {}): Cursor {
    return new Cursor(() => this
      .connect()
      .then(collection => collection?.aggregate(pipeline, opts))
    );
  }

  initializeOrderedBulkOp(opts) {
    return new Bulk(this.name, true, () => this.db.connect(), opts);
  }

  initializeUnorderedBulkOp(opts) {
    return new Bulk(this.name, false, () => this.db.connect(), opts);
  }
}
