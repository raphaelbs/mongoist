import { ObjectId } from 'mongodb';
import type { Db, WriteConcern } from 'mongodb';

const MAX_BULK_SIZE = 1000;

class Command {
  ordered: boolean;
  writeConcern: WriteConcern;
  private cmdNames: object = {};
  private bulkCollections: object = {};

  constructor(ordered: boolean, writeConcern: WriteConcern) {
    this.ordered = ordered;
    this.writeConcern = writeConcern;
  }

  setCmdName(cmdName: string, colName: string) {
    this.cmdNames[cmdName] = colName;
  }

  setBulkCollection(bulkCollection: string) {
    this.bulkCollections[bulkCollection] = [];
  }

  getCmdName(cmdName: string) {
    return this.cmdNames[cmdName];
  }

  getBulkCollection(bulkCollection: string) {
    return this.bulkCollections[bulkCollection];
  }
}

export default class Bulk {
  private readonly colName: string;
  private readonly ordered: boolean;
  private readonly connect: () => Promise<Db | null>;
  private readonly cmds: any[];
  private readonly writeConcern: WriteConcern;
  private readonly cmdKeys = {
    insert: 'nInserted',
    delete: 'nRemoved',
    update: 'nUpserted'
  }
  private currentCmd: Command | null;

  constructor(
    colName: string,
    ordered: boolean,
    connect: () => Promise<Db | null>,
    opts: object & { writeConcern: WriteConcern } = { writeConcern: { w: 1 } }) {
      this.colName = colName;
      this.ordered = ordered;
      this.connect = connect;
      
      this.writeConcern = opts.writeConcern || { w: 1 };

      this.cmds = [];
    }

  ensureCommand(cmdName: string, bulkCollection: string) {
    if (this.currentCmd && (!this.currentCmd.getCmdName(cmdName) || this.currentCmd.getBulkCollection(bulkCollection).length === MAX_BULK_SIZE)) {
      this.cmds.push(this.currentCmd);
      this.currentCmd = null;
    }

    if (!this.currentCmd) {
      this.currentCmd = new Command(this.ordered, this.writeConcern);
      this.currentCmd.setCmdName(cmdName, this.colName);
      this.currentCmd.setBulkCollection(bulkCollection);
    }

    return this.currentCmd;
  }

  find(q) {
    let upsert = false;

    const remove = (limit) => {
      const cmd = this.ensureCommand('delete', 'deletes');
      cmd.getBulkCollection('deletes').push({ q, limit });
    }

    const update = (u, multi) => {
      const cmd = this.ensureCommand('update', 'updates');
      cmd.getBulkCollection('updates').push({ q, u, multi, upsert })
    }

    return new FindSyntax({ 
      update, 
      upsert(upsertValue) { upsert = upsertValue }, 
      remove 
    });
  }

  insert(doc) {
    const cmd = this.ensureCommand('insert', 'documents');

    doc._id = doc._id || new ObjectId();
    cmd.getBulkCollection('documents').push(doc);
  }

  execute() {
    this.pushCurrentCmd();

    const result = {
      writeErrors: [],
      writeConcernErrors: [],
      nInserted: 0,
      nUpserted: 0,
      nMatched: 0,
      nModified: 0,
      nRemoved: 0,
      upserted: []
    }

    const setResult = (cmd, cmdResult) => {
      const cmdKey = Object.keys(cmd)[0];
      result[this.cmdKeys[cmdKey]] += cmdResult.n;
      // Collate and report writeErrors to the result object; forgo doing the same for writeConcernErrors
      // since the node driver throws on writeConcernErrors rather than returning them here.
      if (cmdResult.writeErrors) {
        for (const writeError of cmdResult.writeErrors) {
          result.writeErrors.push(writeError);
        }
      }
    }

    return this
      .connect()
      .then(connection => each(this.cmds, cmd => connection.command(cmd).then(cmdResult => setResult(cmd, cmdResult))))
      .then(() => {
        result.ok = 1;

        return result;
      });
  }

  pushCurrentCmd() {
    if (this.currentCmd) {
      this.cmds.push(this.currentCmd)
    }
  }

  tojson() {
    if (this.currentCmd) {
      this.cmds.push(this.currentCmd);
    }
  
    const obj = {
      nInsertOps: 0,
      nUpdateOps: 0,
      nRemoveOps: 0,
      nBatches: this.cmds.length
    }
  
    this.cmds.forEach(function (cmd) {
      if (cmd.update) {
        obj.nUpdateOps += cmd.updates.length
      } else if (cmd.insert) {
        obj.nInsertOps += cmd.documents.length
      } else if (cmd.delete) {
        obj.nRemoveOps += cmd.deletes.length
      }
    })
  
    return obj
  }

  toString () {
    return JSON.stringify(this.tojson())
  }
}

type FindSyntaxCmds = {
  update: (updObj: object, multi: boolean) => void;
  upsert: (upsertValue: boolean) => void;
  remove: (limit: number) => void;
}

class FindSyntax {
  private readonly cmds: FindSyntaxCmds;

  constructor(cmds: FindSyntaxCmds) {
    this.cmds = cmds;
  }

  upsert() {
    this.cmds.upsert(true);

    return this;
  }

  remove() {
    this.cmds.remove(0);
  }

  removeOne() {
    this.cmds.remove(1);
  }

  update(updObj) {
    this.cmds.update(updObj, true);
  }

  updateOne(updObj) {
    this.cmds.update(updObj, false);
  }

  replaceOne(updObj) {
    this.updateOne(updObj);
  }
}

// TODO: This implementation is a bit whacky recursive implementation. PR anyone?
function each(cmds, executeCmd, idx) {
  idx = idx || 0;

  if (idx < cmds.length) {
    return executeCmd(cmds[idx]).then(() => each(cmds, executeCmd, idx + 1));
  }

  return Promise.resolve();
}
