//
//  SQLPackRatDatabase.m
//  SQLPackRat
//
//  Created by Steven Fisher on 2011-04-29.
//  Copyright 2011 Steven Fisher. All rights reserved.
//

#import "SQLPackRatDatabase.h"

#import "SQLPackRatErrors.h"
#import "SQLPackRatStmt.h"
#import "SQLPackRatTransaction.h"

#ifndef SQLPACKRAT_LOG_ERRORS
#if defined(DEBUG) && DEBUG
#define SQLPACKRAT_LOG_ERRORS DEBUG
#else
#define SQLPACKRAT_LOG_ERRORS 0
#endif
#endif

#ifndef SQLPACKRAT_DISABLE_TRANSACTIONS
#if defined(DEBUG) && DEBUG
#define SQLPACKRAT_DISABLE_TRANSACTIONS DEBUG
#else
#define SQLPACKRAT_DISABLE_TRANSACTIONS 0
#endif
#endif

@interface SQLPackRatDatabase()
@property (nonatomic, readwrite, strong) NSString *path;
@property (nonatomic, readwrite, strong) NSMutableDictionary *functions;
@property (nonatomic, readwrite, strong) NSMutableDictionary *attaches;
@property (nonatomic, readwrite, weak) NSError *lastError;
@end

@implementation SQLPackRatDatabase {
    dispatch_queue_t _background;
}


static inline long fromNSInteger(NSInteger i) {
    return (long)i;
}


- (void)logError: (NSError *)error {
    if ( _lastError != error ) {
        if ( _logErrors ) {
            NSLog( @"%@", error );
        }
        _lastError = error;
    }
}


- (instancetype)init {
    if (( self = [super init] )) {
        _functions = [NSMutableDictionary dictionary];
        _attaches = [NSMutableDictionary dictionary];
        _logErrors = SQLPACKRAT_LOG_ERRORS;
        _transactionsEnabled = !SQLPACKRAT_DISABLE_TRANSACTIONS;
    }
    return self;
}



+ (instancetype)database {
    return [[self alloc] init];
}


- (void)dealloc {
    [self close];
}


+ (NSError *)errorWithCode: (NSInteger)code
                   message: (NSString *)message {
    return [NSError errorWithDomain: SQLPackRatSQL3ErrorDomain
                               code: code
                           userInfo: @{NSLocalizedDescriptionKey:message}];
}


+ (NSError *)errorWithSQLPackRatErrorCode: (NSInteger)errorCode
                              message: (NSString *)message {
    
    return [[self class] errorWithCode:errorCode message:message];
}


- (NSError *)errorWithSQLPackRatErrorCode: (NSInteger)errorCode {
    return [[self class] errorWithCode:errorCode message:@(sqlite3_errmsg(_sqlite3))];
}


- (NSInteger)schemaVersion {
    NSError *error;
    NSDictionary *record = [self firstRecordFromSQL: @"pragma schema_version;"
                           bindingValues: nil
                               withError: &error];
    if ( !record ) {
        [self logError: error];
        return 0;
    }
    NSInteger schemaVersion = [[record objectForKey: @"schema_version"] integerValue];
    return schemaVersion;
}


- (BOOL)openPath: (NSString *)path
           error: (NSError **)outError {
    return [self openPath: path
                    flags: SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
                      vfs: nil
                    error: outError];
}


- (BOOL)openPath: (NSString *)path
           flags: (int)flags
             vfs: (NSString *)VFS
           error: (NSError **)outError {
    [self close];
    self.path = path;
    const char *zPath = [path cStringUsingEncoding: NSUTF8StringEncoding];
    const char *zVFS = [VFS cStringUsingEncoding: NSUTF8StringEncoding];
    int err = sqlite3_open_v2( zPath, &_sqlite3, flags, zVFS );
    if (err != SQLITE_OK) {
        NSError *error = [self errorWithSQLPackRatErrorCode: err];
        [self logError: error];
        if ( outError ) { *outError = error; }
        sqlite3_close( _sqlite3 );
        _sqlite3 = NULL;
        return NO;
    }
    return YES;
}


- (BOOL)attachPath: (NSString *)path
              name: (NSString *)name
             error: (NSError **)outError {
    const char *zPath = [path cStringUsingEncoding: NSUTF8StringEncoding];
    const char *zName = [name cStringUsingEncoding: NSUTF8StringEncoding];
    char *zSQL = sqlite3_mprintf( "ATTACH DATABASE %Q as \"%s\";", zPath, zName );
    NSString *SQL = @(zSQL);
    sqlite3_free( zSQL );
    
    [_attaches setObject: path
                  forKey: name];
    
    NSError *error;
    if ( ![self executeSQL: SQL
          bindingKeyValues: nil
                 withError: &error] ) {
        [self logError: error];
        if ( outError ) { *outError = error; }
        return NO;
    }
    
    return YES;
}


- (void)close {
    if ( _sqlite3 ) {
        if ( sqlite3_close( _sqlite3 ) == SQLITE_OK ) {
            _sqlite3 = NULL;
        }
    }
}


- (SQLPackRatStmt *)stmt {
    return [SQLPackRatStmt stmtWithDatabase: self];
}


- (SQLPackRatStmt *)stmtWithSQL:(NSString *)SQL
           bindingKeyValues: (NSDictionary *)keyValues
                  withError: (NSError **)outError {
    SQLPackRatStmt *stmt = [self stmt];
    NSError *error;
    if (![stmt prepare:SQL remaining:nil withError:&error]) {
        [self logError: error];
        if ( outError ) { *outError = error; }
        return nil;
    }
    if (![stmt bindKeyValues:keyValues withError:&error]) {
        [self logError: error];
        if ( outError ) { *outError = error; }
        return nil;
    }
    return stmt;
}


- (SQLPackRatStmt *)stmtWithSQL:(NSString *)SQL
              bindingValues: (NSArray *)values
                  withError: (NSError **)outError {
    SQLPackRatStmt *stmt = [self stmt];
    NSError *error;
    if (![stmt prepare:SQL remaining:nil withError:&error]) {
        [self logError: error];
        if ( outError ) { *outError = error; }
        return nil;
    }
    if (![stmt bindArray:values withError:&error]) {
        [self logError: error];
        if ( outError ) { *outError = error; }
        return nil;
    }
    return stmt;
}


- (SQLPackRatTransaction *)transactionWithLabel: (NSString *)label {
    return [SQLPackRatTransaction transactionWithDatabase: self
                                                label: label];
}


typedef BOOL(^SQLPackRatBindBlock)(SQLPackRatStmt *statement, NSError **outError);


- (BOOL)executeSQL: (NSString *)SQL
         bindBlock: (SQLPackRatBindBlock)bind
         withError: (NSError **)outError {
    NSError *error;
    SQLPackRatStmt *st = [self stmt];
    NSString *current = SQL;
    for ( ;; ) {
        NSString *rest;
        if ( ![st prepare: current
                remaining: &rest
                withError: &error] ) {
            [self logError: error];
            if ( outError ) { *outError = error; }
            return NO;
        }
        
        if ( ![st haveStmt] ) {
            [st closeWithError: &error];
            return YES;
        }
        
        if ( bind ) {
            if ( !bind( st, &error ) ) {
                [self logError: error];
                if ( outError ) { *outError = error; }
                [st closeWithError: &error];
                return NO;
            }
        }
        BOOL ok = [st skipWithError: &error];
        if ( !ok ) {
            [self logError: error];
            if ( outError ) { *outError = error; }
            [st closeWithError: &error];
            return NO;
        }
        
        [st closeWithError: &error];
        
        current = rest;
    }
}


- (NSNumber *)changesFromSQL: (NSString *)SQL
                   bindBlock: (SQLPackRatBindBlock)bind
                   withError: (NSError **)outError {
    NSError *error;
    SQLPackRatStmt *st = [self stmt];
    NSString *current = SQL;
    NSInteger changes = 0;
    for ( ;; ) {
        NSString *rest;
        if ( ![st prepare: current
                remaining: &rest
                withError: &error] ) {
            [self logError: error];
            if ( outError ) { *outError = error; }
            return nil;
        }
        
        if ( ![st haveStmt] ) {
            [st closeWithError: &error];
            return [NSNumber numberWithInteger: changes];
        }
        
        if ( bind ) {
            if ( !bind( st, &error ) ) {
                [self logError: error];
                if ( outError ) { *outError = error; }
                [st closeWithError: &error];
                return nil;
            }
        }
        BOOL ok = [st skipWithError: &error];
        if ( !ok ) {
            [self logError: error];
            if ( outError ) { *outError = error; }
            [st closeWithError: &error];
            return nil;
        }
        changes += sqlite3_changes(_sqlite3);
        
        [st closeWithError: &error];
        
        current = rest;
    }
}


- (BOOL)executeSQL: (NSString *)SQL
  bindingKeyValues: (NSDictionary *)keyValues
         withError: (NSError **)outError {
    NSError *error;
    SQLPackRatBindBlock bind = ^BOOL(SQLPackRatStmt *stmt, NSError **outE) {
        NSError *e;
        if ( ![stmt bindKeyValues: keyValues
                        withError: &e] ) {
            if ( outE ) { *outE = e; }
            return NO;
        }
        return YES;
    };
    
    if ( ![self executeSQL: SQL
                 bindBlock: bind
                 withError: &error] ) {
        
        [self logError: error];
        if ( outError ) { *outError = error; }
        return NO;
    }
    return YES;
}


- (void)runInBackground:(dispatch_block_t)background {
    @synchronized(self) {
        if (!_background) _background = dispatch_queue_create("SQLPackRatDatabase", 0);
        dispatch_async(_background, ^{
            background();
        });
    }
}


- (void)executeSQL: (NSString *)SQL
  bindingKeyValues: (NSDictionary *)bindings
        completion: (SQLPackRatCompletion)completion {
    [self runInBackground:^{
        NSError *e;
        [self executeSQL:SQL bindingKeyValues:bindings withError:&e];
    }];
}


- (NSNumber *)changesFromSQL: (NSString *)SQL
            bindingKeyValues: (NSDictionary *)keyValues
                   withError: (NSError **)outError {
    NSError *error;
    SQLPackRatBindBlock bind = ^BOOL(SQLPackRatStmt *stmt, NSError **outE) {
        NSError *e;
        if ( ![stmt bindKeyValues: keyValues
                        withError: &e] ) {
            if ( outE ) { *outE = e; }
            return NO;
        }
        return YES;
    };
    
    NSNumber *changes = [self changesFromSQL: SQL
                                   bindBlock: bind
                                   withError: &error];
    if ( !changes ) {
        
        [self logError: error];
        if ( outError ) { *outError = error; }
        return nil;
    }
    return changes;
}


- (NSNumber *)changesFromSQL: (NSString *)SQL
               bindingValues: (NSArray *)values
                   withError: (NSError **)outError {
    NSError *error;
    SQLPackRatBindBlock bind = ^BOOL(SQLPackRatStmt *stmt, NSError **outE) {
        NSError *e;
        if ( ![stmt bindArray: values
                    withError: &e] ) {
            if ( outE ) { *outE = e; }
            return NO;
        }
        return YES;
    };
    
    NSNumber *changes = [self changesFromSQL: SQL
                                   bindBlock: bind
                                   withError: &error];
    if ( !changes ) {
        
        [self logError: error];
        if ( outError ) { *outError = error; }
        return nil;
    }
    return changes;
}


- (NSNumber *)insertUsingSQL: (NSString *)SQL
            bindingKeyValues: (NSDictionary *)keyValues
                   withError: (NSError **)outError {
    NSError *error;
    SQLPackRatBindBlock bind = ^BOOL(SQLPackRatStmt *stmt, NSError **outE) {
        NSError *e;
        if ( ![stmt bindKeyValues: keyValues
                        withError: &e] ) {
            if ( outE ) { *outE = e; }
            return NO;
        }
        return YES;
    };
    
    if ( ![self executeSQL: SQL
                 bindBlock: bind
                 withError: &error] ) {
        
        [self logError: error];
        if ( outError ) { *outError = error; }
        return nil;
    }
    
    sqlite3_int64 rowID = sqlite3_last_insert_rowid( [self sqlite3] );
    return @(rowID);
}



- (NSNumber *)insertUsingSQL: (NSString *)SQL
               bindingValues: (NSArray *)values
                   withError: (NSError **)outError {
    NSError *error;
    SQLPackRatBindBlock bind = ^BOOL(SQLPackRatStmt *stmt, NSError **outE) {
        NSError *e;
        if ( ![stmt bindArray: values
                    withError: &e] ) {
            if ( outE ) { *outE = e; }
            return NO;
        }
        return YES;
    };
    
    if ( ![self executeSQL: SQL
                 bindBlock: bind
                 withError: &error] ) {
        
        [self logError: error];
        if ( outError ) { *outError = error; }
        return nil;
    }
    
    sqlite3_int64 rowID = sqlite3_last_insert_rowid( [self sqlite3] );
    return @(rowID);
}



- (BOOL)executeSQLFromPath: (NSString *)path
          bindingKeyValues: (NSDictionary *)keyValues
                 withError: (NSError **)outError {
    NSError *error;
    NSString *SQL = [NSString stringWithContentsOfFile: path
                                       encoding: NSUTF8StringEncoding
                                          error: &error];
    if ( !SQL ) {
        [self logError: error];
        if ( outError ) { *outError = error; }
        return NO;
    }
    BOOL ok = [self executeSQL: SQL
              bindingKeyValues: keyValues
                     withError: &error];
    if ( !ok ) {
        [self logError: error];
        if ( outError ) { *outError = error; }
        return NO;
    }
    return YES;
}


- (BOOL)executeSQLNamed: (NSString *)name
             fromBundle: (NSBundle *)bundle
       bindingKeyValues: (NSDictionary *)keyValues
              withError: (NSError **)outError {
    NSError *error;
    NSString *SQLPath = [bundle pathForResource: name
                                  ofType: @"sql"];
    if ( ![self executeSQLFromPath: SQLPath
                  bindingKeyValues: keyValues
                         withError: &error] ) {
        [self logError: error];
        if ( outError ) { *outError = error; }
        return NO;
    }
    return YES;
}


static void xFuncGlue( sqlite3_context *context, int argC, sqlite3_value **argsV ) {
    id fn = (__bridge id)sqlite3_user_data( context );
    NSValue *funcValue = [fn objectForKey: @"Func"];
    NSObject *target = [fn objectForKey: @"Target"];
    SEL func;
    [funcValue getValue: &func];
    NSUInteger argCount = argC;
    
    NSMethodSignature *sig = [target methodSignatureForSelector: func];
    NSInvocation *invocation = [NSInvocation invocationWithMethodSignature: sig];
    [invocation setTarget: target];
    [invocation setSelector: func];
    [invocation setArgument: &context atIndex: 2];
    [invocation setArgument: &argCount atIndex: 3];
    [invocation setArgument: &argsV atIndex: 4];
    [invocation invoke];
}


static void xStepGlue( sqlite3_context *context, int argC, sqlite3_value **argsV ) {
    id fn = (__bridge id)sqlite3_user_data( context );
    NSValue *funcValue = [fn objectForKey: @"Step"];
    NSObject *target = [fn objectForKey: @"Target"];
    SEL func;
    [funcValue getValue: &func];
    
    NSMethodSignature *sig = [target methodSignatureForSelector: func];
    NSInvocation *invocation = [NSInvocation invocationWithMethodSignature: sig];
    [invocation setTarget: target];
    [invocation setSelector: func];
    [invocation setArgument: &context atIndex: 2];
    [invocation setArgument: &argC atIndex: 3];
    [invocation setArgument: &argsV atIndex: 4];
    [invocation invoke];
}


static void xFinalGlue( sqlite3_context *context ) {
    id fn = (__bridge id)sqlite3_user_data( context );
    NSValue *funcValue = [fn objectForKey: @"Final"];
    NSObject *target = [fn objectForKey: @"Target"];
    SEL func;
    [funcValue getValue: &func];
    
    NSMethodSignature *sig = [target methodSignatureForSelector: func];
    NSInvocation *invocation = [NSInvocation invocationWithMethodSignature: sig];
    [invocation setTarget: target];
    [invocation setSelector: func];
    [invocation setArgument: &context atIndex: 2];
    [invocation invoke];
}


- (BOOL)removeFunctionNamed: (NSString *)name
                   argCount: (NSInteger)argCount
                  withError: (NSError **)outError {
    const char *nameCStr = [name cStringUsingEncoding: NSUTF8StringEncoding];
    int err = sqlite3_create_function( _sqlite3, nameCStr, (int)argCount, SQLITE_UTF8, NULL, NULL, NULL, NULL );
    if ( err != SQLITE_OK ) {
        NSError *error = [self errorWithSQLPackRatErrorCode: err];
        [self logError: error];
        if ( outError ) { *outError = error; }
        return NO;
    }
    
    NSString *sig = [NSString stringWithFormat: @"%@-%ld", name, fromNSInteger(argCount)];
    [_functions removeObjectForKey: sig];
    
    return YES;
}


- (BOOL)createFunctionNamed: (NSString *)name
                   argCount: (NSInteger)argCount
                     target: (NSObject *)target
                       func: (SEL)function
                       step: (SEL)step
                      final: (SEL)final
                  withError: (NSError **)outError {
    NSString *sig = [NSString stringWithFormat: @"%@-%ld", name, fromNSInteger(argCount)];
    [_functions removeObjectForKey: sig];
    
    NSMutableDictionary *fn;
    if ( function || step || final ) {
        fn = [NSMutableDictionary dictionary];
        [fn setObject: target
               forKey: @"Target"];
        if ( function ) {
            [fn setObject: [NSValue valueWithBytes: &function objCType: @encode( SEL )]
                   forKey: @"Func"];
        }
        if ( step ) {
            [fn setObject: [NSValue valueWithBytes: &step objCType: @encode( SEL )]
                   forKey: @"Step"];
        }
        if ( final ) {
            [fn setObject: [NSValue valueWithBytes: &final objCType: @encode( SEL )]
                   forKey: @"Final"];
        }
        [_functions setObject: fn
                       forKey: sig];
    }
    
    const char *nameCStr = [name cStringUsingEncoding: NSUTF8StringEncoding];
    void *pApp = (__bridge void *)fn;
    void *xFunc = function ? xFuncGlue : NULL;
    void *xStep = step ? xStepGlue : NULL;
    void *xFinal = final ? xFinalGlue : NULL;
    int err = sqlite3_create_function( _sqlite3, nameCStr, (int)argCount, SQLITE_UTF8, pApp, xFunc, xStep, xFinal );
    if ( err != SQLITE_OK ) {
        NSError *error = [self errorWithSQLPackRatErrorCode: err];
        [self logError: error];
        if ( outError ) { *outError = error; }
        return NO;
    }
    
    return YES;
}


- (NSArray *)recordsFromSQL: (NSString *)SQL
              bindingValues: (NSArray *)values
                  withError: (NSError **)outError {
    NSAssert(!(_refuseMainThread && [NSThread isMainThread]), @"Called from main thread");
    
    NSError *error;
    SQLPackRatStmt *stmt = [self stmtWithSQL:SQL bindingValues:values withError:&error];
    if (!stmt) {
        [self logError: error];
        if (outError) *outError = error;
        return nil;
    }
    
    NSArray *contents = [stmt contentsWithError: &error];
    if ( !contents ) {
        [self logError: error];
        if (outError) { *outError = error; }
        [stmt closeWithError: NULL];
        return nil;
    }
    
    [stmt closeWithError: NULL];
    return contents;
}


- (NSArray *)recordsFromSQL: (NSString *)SQL
           bindingKeyValues: (NSDictionary *)keyValues
                  withError: (NSError **)outError {
    NSAssert(!(_refuseMainThread && [NSThread isMainThread]), @"Called from main thread");
    
    NSError *error;
    SQLPackRatStmt *stmt = [self stmtWithSQL:SQL bindingKeyValues:keyValues withError:&error];
    if (!stmt) {
        [self logError: error];
        if (outError) *outError = error;
        return nil;
    }
    
    NSArray *contents = [stmt contentsWithError: &error];
    if ( !contents ) {
        [self logError: error];
        if (outError) { *outError = error; }
        [stmt closeWithError: NULL];
        return nil;
    }
    
    [stmt closeWithError: NULL];
    return contents;
}


- (void)recordsFromSQL: (NSString *)SQL
         bindingValues: (NSArray *)values
            completion: (SQLPackRatRecordsCompletion)completion {
    [self runInBackground:^{
        NSError *e;
        NSArray *records = [self recordsFromSQL:SQL bindingValues:values withError:&e];
        if (records) {
            completion(nil,records);
        } else {
            completion(e,nil);
        }
    }];
}

- (void)recordsFromSQL: (NSString *)SQL
      bindingKeyValues: (NSDictionary *)keyValues
            completion: (SQLPackRatRecordsCompletion)completion {
    [self runInBackground:^{
        NSError *e;
        NSArray *records = [self recordsFromSQL:SQL bindingKeyValues:keyValues withError:&e];
        if (records) {
            completion(nil,records);
        } else {
            completion(e,nil);
        }
    }];
}


- (NSDictionary *)firstRecordFromSQL: (NSString *)SQL
                       bindingValues: (NSArray *)values
                           withError: (NSError **)outError {
    NSError *error;
    SQLPackRatStmt *stmt = [self stmtWithSQL:SQL bindingValues:values withError:&error];
    if (!stmt) {
        [self logError: error];
        if (outError) *outError = error;
        return nil;
    }
    
    NSDictionary *record = [stmt nextRecord:&error];
    if (!record) {
        [self logError: error];
        if (outError) *outError = error;
        [stmt closeWithError: NULL];
        return nil;
    }

    [stmt closeWithError: NULL];
    return record;
}


- (NSDictionary *)firstRecordFromSQL: (NSString *)SQL
                    bindingKeyValues: (NSDictionary *)keyValues
                           withError: (NSError **)outError {
    NSError *error;
    SQLPackRatStmt *stmt = [self stmtWithSQL:SQL bindingKeyValues:keyValues withError:&error];
    if (!stmt) {
        [self logError: error];
        if (outError) *outError = error;
        return nil;
    }
    
    NSDictionary *record = [stmt nextRecord:&error];
    if (!record) {
        [self logError: error];
        if (outError) *outError = error;
        [stmt closeWithError: NULL];
        return nil;
    }
    
    [stmt closeWithError: NULL];
    return record;
}


- (void)firstRecordFromSQL: (NSString *)SQL
             bindingValues: (NSArray *)values
                completion: (SQLPackRatRecordCompletion)completion {
    
    [self runInBackground:^{
        NSError *e;
        NSDictionary *record = [self firstRecordFromSQL:SQL bindingValues:values withError:&e];
        if (record) {
            completion(nil,record);
        } else {
            completion(e,nil);
        }
    }];
}


- (void)firstRecordFromSQL: (NSString *)SQL
          bindingKeyValues: (NSDictionary *)keyValues
                completion: (SQLPackRatRecordCompletion)completion {
    [self runInBackground:^{
        NSError *e;
        NSDictionary *record = [self firstRecordFromSQL:SQL bindingKeyValues:keyValues withError:&e];
        if (record) {
            completion(nil,record);
        } else {
            completion(e,nil);
        }
    }];
}


- (NSNumber *)insertOrReplace: (NSDictionary *)keyValues
                    intoTable: (NSString *)table
                    withError: (NSError **)error {
    
    NSArray *columns = [keyValues allKeys];
    NSMutableArray *bindings = [NSMutableArray array];
    NSMutableArray *values = [NSMutableArray array];
    for (NSString *columnNames in columns) {
        [bindings addObject:@"?"];
        [values addObject:keyValues[columnNames]];
    }
    
    NSError *e;
    NSString *SQL = [NSString stringWithFormat:@"INSERT OR REPLACE INTO \"%@\"(\"%@\") VALUES(%@);", table, [columns componentsJoinedByString:@"\",\""], [bindings componentsJoinedByString:@","]];
    
    NSNumber *result = [self insertUsingSQL:SQL bindingValues:values withError:&e];
    if (!result) {
        if (error) *error = e;
        return nil;
    }
    
    return result;
}


- (void)insertOrReplace: (NSDictionary *)values
                    intoTable: (NSString *)table
                   completion: (SQLPackRatInsertCompletion)completion {
    [self runInBackground:^{
        NSError *e;
        NSNumber *record = [self insertOrReplace:values intoTable:table withError:&e];
        if (record) {
            completion(nil,record);
        } else {
            completion(e,nil);
        }
    }];
}


- (NSNumber *)insertOrAbort: (NSDictionary *)keyValues
                  intoTable: (NSString *)table
                  withError: (NSError **)error {
    
    NSArray *columns = [keyValues allKeys];
    NSMutableArray *bindings = [NSMutableArray array];
    NSMutableArray *values = [NSMutableArray array];
    for (NSString *columnNames in columns) {
        [bindings addObject:@"?"];
        [values addObject:keyValues[columnNames]];
    }
    
    NSError *e;
    NSString *SQL = [NSString stringWithFormat:@"INSERT OR ABORT INTO \"%@\"(\"%@\") VALUES(%@);", table, [columns componentsJoinedByString:@"\",\""], [bindings componentsJoinedByString:@","]];
    
    NSNumber *result = [self insertUsingSQL:SQL bindingValues:values withError:&e];
    if (!result) {
        if (error) *error = e;
        return nil;
    }
    
    return result;
}


- (void)insertOrAbort: (NSDictionary *)values
                  intoTable: (NSString *)table
                 completion: (SQLPackRatInsertCompletion)completion {
    [self runInBackground:^{
        NSError *e;
        NSNumber *record = [self insertOrAbort:values intoTable:table withError:&e];
        if (record) {
            completion(nil,record);
        } else {
            completion(e,nil);
        }
    }];
}


- (NSString *)description {
    NSMutableString *str = [NSMutableString stringWithFormat: @"sqlite3 \"%@\"\n", _path];
    for (NSString *name in _attaches) {
        NSString *path = [_attaches objectForKey: name];
        [str appendFormat: @"ATTACH '%@' AS %@;\n", path, name];
    }
    return str;
}


- (BOOL)wrapInTransactionContext: (NSString *)context
                           block: (SQLPackRatAction)block
                       withError: (NSError **)outError {
    NSError *error;
    
    SQLPackRatTransaction *transaction = _transactionsEnabled ? [self transactionWithLabel: context] : nil;
    if ( transaction && ![transaction beginImmediateWithError: &error] ) {
        if ( outError ) { *outError = error; }
        return NO;
    }
    
    BOOL ok = block( &error );
    if ( !ok ) {
        if ( outError ) { *outError = error; }
        [transaction rollbackWithError: &error];
        return NO;
    }
    
    if ( transaction && ![transaction commitWithError: &error] ) {
        if ( outError ) { *outError = error; }
        [transaction rollbackWithError: &error];
        return NO;
    }
    
    return YES;
}


- (BOOL)backupTo: (SQLPackRatDatabase *)destination withError:(NSError **)error {
    sqlite3 *other = destination.sqlite3;
    sqlite3_backup *backup = sqlite3_backup_init(other, "main", _sqlite3, "main");
    if (!backup) {
        NSError *e = [[destination class] errorWithCode:sqlite3_errcode(other) message:@(sqlite3_errmsg(other))];
        [destination logError:e];
        if (error) *error = e;
        return NO;
    }
    
    int result;
    do {
        result = sqlite3_backup_step(backup, -1);
    } while (result == SQLITE_OK);
    if ( result != SQLITE_DONE ) {
        NSError *e = [[destination class] errorWithCode:sqlite3_errcode(other) message:@(sqlite3_errmsg(other))];
        [destination logError:e];
        if (error) *error = e;
        sqlite3_backup_finish(backup);
        return NO;
    }
    
    result = sqlite3_backup_finish(backup);
    if ( result != SQLITE_OK ) {
        NSError *e = [[destination class] errorWithCode:sqlite3_errcode(other) message:@(sqlite3_errmsg(other))];
        [destination logError:e];
        if (error) *error = e;
        return NO;
    }
    
    return YES;
}

@end
