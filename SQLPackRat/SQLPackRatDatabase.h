//
//  SQLPackRatDatabase.h
//  SQLPackRat
//
//  Created by Steven Fisher on 2011-04-29.
//  Copyright 2011 Steven Fisher. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <sqlite3.h>

@class SQLPackRatDatabase;
@class SQLPackRatStmt;
@class SQLPackRatTransaction;

typedef BOOL(^SQLPackRatAction)(NSError **outError);
typedef void(^SQLPackRatCompletion)(NSError *error);
typedef void(^SQLPackRatInsertCompletion)(NSError *error, NSNumber *record);
typedef void(^SQLPackRatRecordCompletion)(NSError *error, NSDictionary *record);
typedef void(^SQLPackRatRecordsCompletion)(NSError *error, NSArray *records);

@interface SQLPackRatDatabase : NSObject

@property (nonatomic, readonly, assign) sqlite3 *sqlite3;
@property (nonatomic, readwrite, assign) BOOL refuseMainThread;
@property (nonatomic, readwrite, assign) BOOL transactionsEnabled;

+ (instancetype)database;

- (NSInteger)schemaVersion;

- (BOOL)openPath: (NSString *)path
           error: (NSError **)outError;

- (BOOL)openPath: (NSString *)path
           flags: (int)flags
             vfs: (NSString *)VFS
           error: (NSError **)outError;

- (BOOL)attachPath: (NSString *)path
              name: (NSString *)name
             error: (NSError **)outError;

- (void)close;

- (SQLPackRatStmt *)stmt;

- (SQLPackRatStmt *)stmtWithSQL:(NSString *)SQL
           bindingKeyValues: (NSDictionary *)keyValues
                  withError: (NSError **)outError;

- (SQLPackRatStmt *)stmtWithSQL:(NSString *)SQL
              bindingValues: (NSArray *)values
                  withError: (NSError **)outError;

- (SQLPackRatTransaction *)transactionWithLabel: (NSString *)label;

- (BOOL)executeSQL: (NSString *)SQL
  bindingKeyValues: (NSDictionary *)bindings
         withError: (NSError **)outError;

- (void)executeSQL: (NSString *)SQL
  bindingKeyValues: (NSDictionary *)bindings
        completion: (SQLPackRatCompletion)completion;

- (NSNumber *)changesFromSQL: (NSString *)SQL
            bindingKeyValues: (NSDictionary *)keyValues
                   withError: (NSError **)outError;

- (NSNumber *)changesFromSQL: (NSString *)SQL
               bindingValues: (NSArray *)values
                   withError: (NSError **)outError;

- (NSNumber *)insertUsingSQL: (NSString *)SQL
            bindingKeyValues: (NSDictionary *)keyValues
                   withError: (NSError **)outError;

- (NSNumber *)insertUsingSQL: (NSString *)SQL
               bindingValues: (NSArray *)values
                   withError: (NSError **)outError;

- (BOOL)executeSQLFromPath: (NSString *)path
          bindingKeyValues: (NSDictionary *)keyValues
                 withError: (NSError **)outError;

- (BOOL)executeSQLNamed: (NSString *)name
             fromBundle: (NSBundle *)bundle
       bindingKeyValues: (NSDictionary *)keyValues
              withError: (NSError **)outError;

- (BOOL)removeFunctionNamed: (NSString *)name
                   argCount: (NSInteger)argCount
                  withError: (NSError **)outError;

- (BOOL)createFunctionNamed: (NSString *)name
                   argCount: (NSInteger)argCount
                     target: (NSObject *)target
                       func: (SEL)function
                       step: (SEL)step
                      final: (SEL)final
                  withError: (NSError **)outError;

- (NSArray *)recordsFromSQL: (NSString *)SQL
              bindingValues: (NSArray *)values
                  withError: (NSError **)outError;

- (NSArray *)recordsFromSQL: (NSString *)SQL
           bindingKeyValues: (NSDictionary *)keyValues
                  withError: (NSError **)outError;

- (void)recordsFromSQL: (NSString *)SQL
         bindingValues: (NSArray *)values
            completion: (SQLPackRatRecordsCompletion)completion;

- (void)recordsFromSQL: (NSString *)SQL
      bindingKeyValues: (NSDictionary *)keyValues
            completion: (SQLPackRatRecordsCompletion)completion;

- (NSDictionary *)firstRecordFromSQL: (NSString *)SQL
                       bindingValues: (NSArray *)values
                           withError: (NSError **)outError;

- (NSDictionary *)firstRecordFromSQL: (NSString *)SQL
                    bindingKeyValues: (NSDictionary *)keyValues
                           withError: (NSError **)outError;

- (void)firstRecordFromSQL: (NSString *)SQL
             bindingValues: (NSArray *)values
                completion: (SQLPackRatRecordCompletion)completion;

- (void)firstRecordFromSQL: (NSString *)SQL
          bindingKeyValues: (NSDictionary *)keyValues
                completion: (SQLPackRatRecordCompletion)completion;

- (NSNumber *)insertOrReplace: (NSDictionary *)values
                    intoTable: (NSString *)table
                    withError: (NSError **)error;

- (void)insertOrReplace: (NSDictionary *)values
              intoTable: (NSString *)table
             completion: (SQLPackRatInsertCompletion)completion;

- (NSNumber *)insertOrAbort: (NSDictionary *)values
                  intoTable: (NSString *)table
                  withError: (NSError **)error;

- (void)insertOrAbort: (NSDictionary *)values
            intoTable: (NSString *)table
           completion: (SQLPackRatInsertCompletion)completion;

- (BOOL)wrapInTransactionContext: (NSString *)context
                           block: (SQLPackRatAction)block
                       withError: (NSError **)outError;

@property (nonatomic, assign) BOOL logErrors;


- (BOOL)backupTo: (SQLPackRatDatabase *)destination
       withError: (NSError **)error;

@end
