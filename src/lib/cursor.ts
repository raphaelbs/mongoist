import { Readable } from 'stream';
import type { FindCursor } from 'mongodb';

export default class Cursor extends Readable {
  cursorFactory: () => Promise<Cursor & FindCursor>;
  _options: object;
  _flags: object;
  cursor: Cursor & FindCursor;

  constructor(cursorFactory) {
    super({objectMode: true, highWaterMark: 0});

    this.cursorFactory = cursorFactory;

    // Currently mongodb cursor options itcount, readPref, showDiskLoc are not implemented - and some other
    // See https://docs.mongodb.com/manual/reference/method/js-cursor/
    const operations = ['batchSize', 'hint', 'limit', 'maxTimeMS', 'max', 'min', 'skip', 'snapshot', 'sort', 'collation'];

    this._options = {};
    this._flags = {};

    operations.forEach((operation) => {
      Cursor.prototype[operation] = (data) => {
        this._options[operation] = data;
        return this;
      }
    });
  }

  map(fn: (doc) => never) {
    const result = []

    return new Promise((resolve) => {
      const loop = () => {
        this.next()
          .then(doc => {
            if (!doc) { return resolve(result); }

            result.push(fn(doc));

            // Avoid maximum call stack size exceeded errors
            setImmediate(loop);
          });
      }

      loop();
    });
  }

  forEach(fn: (doc) => void): Promise<any> {
    return new Promise((resolve) => {
      const loop = () => {
        this.next()
          .then(doc => {
            if (!doc) { return resolve(undefined); }

            fn(doc);

            // Avoid maximum call stack size exceeded errors
            setImmediate(loop);
          });
      }

      loop();
    });
  }

  /** @deprecated Please use collection.count instead */
  count() {
    return this.getCursor().then(cursor => cursor.count(this._options));
  }

  size() {
    return this.getCursor().then(cursor => cursor.count(this._options));
  }

  explain() {
    return this.getCursor().then(cursor => cursor.explain());
  }

  destroy() {
    return this.close();
  }

  close() {
    return this.getCursor().then(cursor => cursor.close());
  }

  next(): Promise<any> {
    return this.getCursor().then(cursor => {
      if (cursor.closed || cursor.killed) {
        return null;
      }

      return cursor.next();
    });
  }

  hasNext(): Promise<boolean> {
    return this.getCursor().then(cursor => !cursor.closed && cursor.hasNext());
  }

  rewind() {
    return this.getCursor().then(cursor => cursor.rewind());
  }

  toArray() {
    return this.getCursor().then(cursor => cursor.toArray());
  }

  addCursorFlag(flag, value) {
    this._flags[flag] = value;
    return this;
  }

  _read() {
    return this.next()
      .then(doc => this.push(doc))
      .catch(err => this.emit('error', err));
  }


  getCursor(): Promise<FindCursor> {
    if (this.cursor) {
      return Promise.resolve(this.cursor);
    }

    return this
      .cursorFactory()
      .then(cursor => {
        Object.keys(this._options).forEach(key => {
          cursor = cursor[key](this._options[key]);
        });
        Object.keys(this._flags).forEach(key => {
          cursor.addCursorFlag(key, this._flags[key]);
        });

        cursor.on('close', (...args) => this.emit('close', ...args));

        this.cursor = cursor;

        return cursor;
      });
  }
}