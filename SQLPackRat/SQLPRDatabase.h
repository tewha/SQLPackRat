//
//  SQLPRDatabase.h
//  SQLPackRat
//
//  Created by Steven Fisher on 2011-04-29.
//  Copyright 2011 Steven Fisher. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <sqlite3.h>

#ifndef COMPATIBILITY_MODE
#define COMPATIBILITY_MODE 0
#endif

@class SQLPRDatabase;
@class SQLPRStmt;
@class SQLPRTransaction;

typedef BOOL(^SQLPRTransactionBlock)(NSError **outError);
typedef void(^SQLPRExecuteCompletionBlock)(NSError *error);
typedef void(^SQLPRInsertCompletionBlock)(NSError *error, NSNumber *record);
typedef void(^SQLPRSelectCompletionBlock)(NSError *error, NSDictionary<NSString *, id> *record);
typedef void(^SQLPRSelectFirstCompletionBlock)(NSError *error, NSArray *records);
typedef void(^SQLPRCustomFuncBlock)(sqlite3_context *context, int argC, sqlite3_value **argsV);
typedef void(^SQLPRCustomStepBlock)(sqlite3_context *context, int argC, sqlite3_value **argsV);
typedef void(^SQLPRCustomFinalBlock)(sqlite3_context *context);

/** A SQLPRDatabase tracks a database from open to close.
 
 Each SQLPRStatement maintains a strong reference to the SQLPRDatabase. This may seem a little backwards, but it keeps the database open as long as a statement references it. */
@interface SQLPRDatabase : NSObject

@property (nonatomic, readonly, assign) sqlite3 *sqlite3;
@property (nonatomic, readwrite, assign) BOOL refuseMainThread;
@property (nonatomic, readwrite, assign) BOOL transactionsEnabled;
@property (nonatomic, assign) BOOL logErrors;

- (instancetype)initWithPath:(NSString *)path flags:(int)flags vfs:(NSString *)VFS error:(NSError **)outError;

- (NSInteger)schemaVersion;

- (BOOL)openPath:(NSString *)path error:(NSError **)outError;

- (BOOL)openPath:(NSString *)path flags:(int)flags vfs:(NSString *)VFS error:(NSError **)outError;

- (BOOL)attachPath:(NSString *)path name:(NSString *)name error:(NSError **)outError;

- (void)close;

/* SQL statement creation. */

#if COMPATIBILITY_MODE
- (SQLPRStmt *)newStmt;
#endif

- (SQLPRStmt *)newStmtWithSQL:(NSString *)SQL bindingKeyValues:(NSDictionary<NSString *, id> *)keyValues withError:(NSError **)outError;

- (SQLPRStmt *)newStmtWithSQL:(NSString *)SQL bindingKeyValues:(NSDictionary<NSString *, id> *)keyValues tail:(NSString **)tail withError:(NSError **)outError;

- (SQLPRStmt *)newStmtWithSQL:(NSString *)SQL bindingValues:(NSArray *)values tail:(NSString **)tail withError:(NSError **)outError;

- (SQLPRStmt *)newStmtWithSQL:(NSString *)SQL bindingValues:(NSArray *)values withError:(NSError **)outError;

/* SQL transaction creation. */

- (SQLPRTransaction *)newTransactionWithLabel:(NSString *)label;

/* SQL statement execution */
- (BOOL)executeSQL:(NSString *)SQL bindingKeyValues:(NSDictionary<NSString *, id> *)bindings withError:(NSError **)outError;

- (void)executeSQL:(NSString *)SQL bindingKeyValues:(NSDictionary<NSString *, id> *)bindings completion:(SQLPRExecuteCompletionBlock)completion;

- (BOOL)executeSQLFromPath:(NSString *)path bindingKeyValues:(NSDictionary<NSString *, id> *)keyValues withError:(NSError **)outError;

- (BOOL)executeSQLNamed:(NSString *)name fromBundle:(NSBundle *)bundle bindingKeyValues:(NSDictionary<NSString *, id> *)keyValues withError:(NSError **)outError;

- (NSNumber *)changesFromSQL:(NSString *)SQL bindingKeyValues:(NSDictionary<NSString *, id> *)keyValues withError:(NSError **)outError;

- (NSNumber *)changesFromSQL:(NSString *)SQL bindingValues:(NSArray *)values withError:(NSError **)outError;

- (NSNumber *)insertUsingSQL:(NSString *)SQL bindingKeyValues:(NSDictionary<NSString *, id> *)keyValues withError:(NSError **)outError;

- (NSNumber *)insertUsingSQL:(NSString *)SQL bindingValues:(NSArray *)values withError:(NSError **)outError;

- (NSArray *)recordsFromSQL:(NSString *)SQL bindingValues:(NSArray *)values withError:(NSError **)outError;

- (NSArray *)recordsFromSQL:(NSString *)SQL bindingKeyValues:(NSDictionary<NSString *, id> *)keyValues withError:(NSError **)outError;

- (void)recordsFromSQL:(NSString *)SQL bindingValues:(NSArray *)values completion:(SQLPRSelectFirstCompletionBlock)completion;

- (void)recordsFromSQL:(NSString *)SQL bindingKeyValues:(NSDictionary<NSString *, id> *)keyValues completion:(SQLPRSelectFirstCompletionBlock)completion;

- (NSDictionary<NSString *, id> *)firstRecordFromSQL:(NSString *)SQL bindingValues:(NSArray *)values withError:(NSError **)outError;

- (NSDictionary<NSString *, id> *)firstRecordFromSQL:(NSString *)SQL bindingKeyValues:(NSDictionary<NSString *, id> *)keyValues withError:(NSError **)outError;

- (void)firstRecordFromSQL:(NSString *)SQL bindingValues:(NSArray *)values completion:(SQLPRSelectCompletionBlock)completion;

- (void)firstRecordFromSQL:(NSString *)SQL bindingKeyValues:(NSDictionary<NSString *, id> *)keyValues completion:(SQLPRSelectCompletionBlock)completion;

- (NSArray *)insertRecords:(NSArray *)records intoTable:(NSString *)table withError:(NSError **)error;

- (NSNumber *)insertOrReplace:(NSDictionary<NSString *, id> *)values intoTable:(NSString *)table withError:(NSError **)error;

- (void)insertOrReplace:(NSDictionary<NSString *, id> *)values intoTable:(NSString *)table completion:(SQLPRInsertCompletionBlock)completion;

- (NSNumber *)insertOrAbort:(NSDictionary<NSString *, id> *)values intoTable:(NSString *)table withError:(NSError **)error;

- (void)insertOrAbort:(NSDictionary<NSString *, id> *)values intoTable:(NSString *)table completion:(SQLPRInsertCompletionBlock)completion;

- (BOOL)wrapInTransactionContext:(NSString *)context block:(SQLPRTransactionBlock)block withError:(NSError **)outError;

/* Custom functions */

- (BOOL)addFunctionNamed:(NSString *)name argCount:(NSInteger)argCount target:(NSObject *)target func:(SEL)function step:(SEL)step final:(SEL)final withError:(NSError **)outError;

- (BOOL)addFunctionNamed:(NSString *)name argCount:(NSInteger)argCount func:(SQLPRCustomFuncBlock)function step:(SQLPRCustomStepBlock)step final:(SQLPRCustomFinalBlock)final withError:(NSError **)outError;

- (BOOL)removeFunctionNamed:(NSString *)name argCount:(NSInteger)argCount withError:(NSError **)outError;

/* Other */

- (BOOL)backupTo:(SQLPRDatabase *)destination withError:(NSError **)error;

@end
