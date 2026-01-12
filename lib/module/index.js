"use strict";

import { NativeModules } from 'react-native';
function isRemoteDebuggingInChrome() {
  // Remote debugging in Chrome is not supported in bridgeless
  if ('RN$Bridgeless' in global && RN$Bridgeless === true) return false;
  return __DEV__ && typeof global.nativeCallSyncHook === 'undefined';
}
if (global.__QuickSQLiteProxy == null) {
  const QuickSQLiteModule = NativeModules.QuickSQLite;
  if (QuickSQLiteModule == null) {
    throw new Error('Base quick-sqlite module not found. Maybe try rebuilding the app.');
  }

  // Check if we are running on-device (JSI)
  if (isRemoteDebuggingInChrome() || QuickSQLiteModule.install == null) {
    throw new Error('Failed to install react-native-quick-sqlite: React Native is not running on-device. QuickSQLite can only be used when synchronous method invocations (JSI) are possible. If you are using a remote debugger (e.g. Chrome), switch to an on-device debugger (e.g. Flipper) instead.');
  }

  // Call the synchronous blocking install() function
  const result = QuickSQLiteModule.install();
  if (result !== true) {
    throw new Error(`Failed to install react-native-quick-sqlite: The native QuickSQLite Module could not be installed! Looks like something went wrong when installing JSI bindings: ${result}`);
  }

  // Check again if the constructor now exists. If not, throw an error.
  if (global.__QuickSQLiteProxy == null) {
    throw new Error('Failed to install react-native-quick-sqlite, the native initializer function does not exist. Are you trying to use QuickSQLite from different JS Runtimes?');
  }
}
const proxy = global.__QuickSQLiteProxy;
export const QuickSQLite = proxy;

/**
 * Object returned by SQL Query executions {
 *  insertId: Represent the auto-generated row id if applicable
 *  rowsAffected: Number of affected rows if result of a update query
 *  message: if status === 1, here you will find error description
 *  rows: if status is undefined or 0 this object will contain the query results
 * }
 *
 * @interface QueryResult
 */

/**
 * Column metadata
 * Describes some information about columns fetched by the query
 */

/**
 * Allows the execution of bulk of sql commands
 * inside a transaction
 * If a single query must be executed many times with different arguments, its preferred
 * to declare it a single time, and use an array of array parameters.
 */

/**
 * status: 0 or undefined for correct execution, 1 for error
 * message: if status === 1, here you will find error description
 * rowsAffected: Number of affected rows if status == 0
 */

/**
 * Result of loading a file and executing every line as a SQL command
 * Similar to BatchQueryResult
 */

const locks = {};

// Enhance some host functions

// Add 'item' function to result object to allow the sqlite-storage typeorm driver to work
const enhanceQueryResult = result => {
  // Add 'item' function to result object to allow the sqlite-storage typeorm driver to work
  if (result.rows == null) {
    result.rows = {
      _array: [],
      length: 0,
      item: idx => result.rows?._array[idx]
    };
  } else {
    result.rows.item = idx => result.rows?._array[idx];
  }
};
const _open = QuickSQLite.open;
QuickSQLite.open = (dbName, location) => {
  _open(dbName, location);
  locks[dbName] = {
    queue: [],
    inProgress: false
  };
};
const _close = QuickSQLite.close;
QuickSQLite.close = dbName => {
  _close(dbName);
  delete locks[dbName];
};
const _execute = QuickSQLite.execute;
QuickSQLite.execute = (dbName, query, params) => {
  const result = _execute(dbName, query, params);
  enhanceQueryResult(result);
  return result;
};
const _executeAsync = QuickSQLite.executeAsync;
QuickSQLite.executeAsync = async (dbName, query, params) => {
  const res = await _executeAsync(dbName, query, params);
  enhanceQueryResult(res);
  return res;
};

// @ts-ignore
QuickSQLite.transaction = async (dbName, fn) => {
  if (!locks[dbName]) throw Error(`Quick SQLite Error: No lock found on db: ${dbName}`);
  let isFinalized = false;

  // Local transaction context object implementation
  const execute = (query, params) => {
    if (isFinalized) {
      throw Error(`Quick SQLite Error: Cannot execute query on finalized transaction: ${dbName}`);
    }
    return QuickSQLite.execute(dbName, query, params);
  };
  const executeAsync = (query, params) => {
    if (isFinalized) {
      throw Error(`Quick SQLite Error: Cannot execute query on finalized transaction: ${dbName}`);
    }
    return QuickSQLite.executeAsync(dbName, query, params);
  };
  const commit = () => {
    if (isFinalized) {
      throw Error(`Quick SQLite Error: Cannot execute commit on finalized transaction: ${dbName}`);
    }
    const result = QuickSQLite.execute(dbName, 'COMMIT');
    isFinalized = true;
    return result;
  };
  const rollback = () => {
    if (isFinalized) {
      throw Error(`Quick SQLite Error: Cannot execute rollback on finalized transaction: ${dbName}`);
    }
    const result = QuickSQLite.execute(dbName, 'ROLLBACK');
    isFinalized = true;
    return result;
  };
  async function run() {
    try {
      await QuickSQLite.executeAsync(dbName, 'BEGIN TRANSACTION');
      await fn({
        commit,
        execute,
        executeAsync,
        rollback
      });
      if (!isFinalized) commit();
    } catch (executionError) {
      if (!isFinalized) {
        try {
          rollback();
        } catch (rollbackError) {
          throw rollbackError;
        }
      }
      throw executionError;
    } finally {
      // @ts-ignore
      locks[dbName].inProgress = false;
      isFinalized = false;
      startNextTransaction(dbName);
    }
  }
  return await new Promise((resolve, reject) => {
    const tx = {
      start: () => {
        run().then(resolve).catch(reject);
      }
    };

    // @ts-ignore
    locks[dbName].queue.push(tx);
    startNextTransaction(dbName);
  });
};
const startNextTransaction = dbName => {
  if (!locks[dbName]) throw Error(`Lock not found for db: ${dbName}`);
  if (locks[dbName].inProgress) {
    // Transaction is already in process bail out
    return;
  }
  if (locks[dbName].queue.length > 0) {
    locks[dbName].inProgress = true;
    const tx = locks[dbName].queue.shift();
    setImmediate(() => {
      // @ts-ignore
      tx.start();
    });
  }
};

//   _________     _______  ______ ____  _____  __  __            _____ _____
//  |__   __\ \   / /  __ \|  ____/ __ \|  __ \|  \/  |     /\   |  __ \_   _|
//     | |   \ \_/ /| |__) | |__ | |  | | |__) | \  / |    /  \  | |__) || |
//     | |    \   / |  ___/|  __|| |  | |  _  /| |\/| |   / /\ \ |  ___/ | |
//     | |     | |  | |    | |___| |__| | | \ \| |  | |  / ____ \| |    _| |_
//     |_|     |_|  |_|    |______\____/|_|  \_\_|  |_| /_/    \_\_|   |_____|

/**
 * DO NOT USE THIS! THIS IS MEANT FOR TYPEORM
 * If you are looking for a convenience wrapper use `connect`
 */
export const typeORMDriver = {
  openDatabase: (options, ok, fail) => {
    try {
      QuickSQLite.open(options.name, options.location);
      const connection = {
        executeSql: async (sql, params, okExecute, okFail) => {
          try {
            const response = await QuickSQLite.executeAsync(options.name, sql, params);
            enhanceQueryResult(response);
            okExecute(response);
          } catch (e) {
            // @ts-ignore
            okFail(e);
          }
        },
        transaction: fn => {
          return QuickSQLite.transaction(options.name, fn);
        },
        close: (okClose, failClose) => {
          try {
            QuickSQLite.close(options.name);
            okClose();
          } catch (e) {
            failClose(e);
          }
        },
        attach: (dbNameToAttach, alias, location, callback) => {
          QuickSQLite.attach(options.name, dbNameToAttach, alias, location);
          callback();
        },
        // @ts-ignore
        detach: (alias, callback) => {
          QuickSQLite.detach(options.name, alias);
          callback();
        }
      };
      ok(connection);
      return connection;
    } catch (e) {
      // @ts-ignore
      fail(e);
    }
  }
};
export const open = options => {
  QuickSQLite.open(options.name, options.location);
  return {
    close: () => QuickSQLite.close(options.name),
    delete: () => QuickSQLite.delete(options.name, options.location),
    attach: (dbNameToAttach, alias, location) => QuickSQLite.attach(options.name, dbNameToAttach, alias, location),
    detach: alias => QuickSQLite.detach(options.name, alias),
    transaction: fn => QuickSQLite.transaction(options.name, fn),
    execute: (query, params) => QuickSQLite.execute(options.name, query, params),
    executeAsync: (query, params) => QuickSQLite.executeAsync(options.name, query, params),
    executeBatch: commands => QuickSQLite.executeBatch(options.name, commands),
    executeBatchAsync: commands => QuickSQLite.executeBatchAsync(options.name, commands),
    loadFile: location => QuickSQLite.loadFile(options.name, location),
    loadFileAsync: location => QuickSQLite.loadFileAsync(options.name, location)
  };
};
//# sourceMappingURL=index.js.map