//
//  SQLPRDatabase.m
//  SQLPackRat
//
//  Created by Steven Fisher on 2011-04-29.
//  Copyright 2011 Steven Fisher. All rights reserved.
//

#import "SQLPRDatabase.h"

#import "SQLPRErrors.h"
#import "SQLPRStmt.h"
#import "SQLPRTransaction.h"

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

static NSString *TargetKey = @"Target";
static NSString *FuncSelectorKey = @"FuncSelector";
static NSString *StepSelectorKey = @"StepSelector";
static NSString *FinalSelectorKey = @"FinalSelector";
static NSString *FuncBlockKey = @"FuncBlock";
static NSString *StepBlockKey = @"StepBlock";
static NSString *FinalBlockKey = @"FinalBlock";

@interface SQLPRDatabase ()
@property (nonatomic, readwrite, strong) NSString *path;
@property (nonatomic, readwrite, strong) NSMutableDictionary *functions;
@property (nonatomic, readwrite, strong) NSMutableDictionary *attaches;
@property (nonatomic, readwrite, weak) NSError *lastError;
@end

@implementation SQLPRDatabase {
    dispatch_queue_t _background;
}


static inline void SetError(NSError **error, NSError *e) {
    if (error) *error = e;
}


static inline long fromNSInteger(NSInteger i) {
    return (long)i;
}


- (void)logError:(NSError *)error {
    if (_lastError != error) {
        if (_logErrors) {
            NSLog(@"%@", error);
        }
        _lastError = error;
    }
}


- (instancetype)init {
    self = [super init];
    
    _functions = [NSMutableDictionary dictionary];
    _attaches = [NSMutableDictionary dictionary];
    _logErrors = SQLPACKRAT_LOG_ERRORS;
    _transactionsEnabled = !SQLPACKRAT_DISABLE_TRANSACTIONS;
    
    return self;
}


- (instancetype)initWithPath:(NSString *)path flags:(int)flags vfs:(NSString *)VFS error:(NSError **)outError {
    self = [self init];
    if (!self) {
        return nil;
    }
    
    NSError *e;
    if (![self openPath:path flags:flags vfs:VFS error:&e]) {
        return nil;
    }
    
    return self;
}


- (void)dealloc {
    [self close];
}


+ (NSError *)errorWithCode:(NSInteger)code message:(NSString *)message {
    return [NSError errorWithDomain:SQLPRSQL3ErrorDomain code:code userInfo:@{NSLocalizedDescriptionKey:message}];
}


+ (NSError *)errorWithSQL3ErrorCode:(NSInteger)errorCode message:(NSString *)message {
    return [[self class] errorWithCode:errorCode message:message];
}


- (NSError *)errorWithSQL3ErrorCode:(NSInteger)errorCode {
    return [[self class] errorWithCode:errorCode message:@(sqlite3_errmsg(_sqlite3))];
}


- (NSInteger)schemaVersion {
    NSError *error;
    NSDictionary *record = [self firstRecordFromSQL:@"pragma schema_version;" bindingValues:nil withError:&error];
    if (!record) {
        [self logError:error];
        return 0;
    }
    NSInteger schemaVersion = [[record objectForKey:@"schema_version"] integerValue];
    return schemaVersion;
}


- (BOOL)openPath:(NSString *)path error:(NSError **)outError {
    return [self openPath:path flags:SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX vfs:nil error:outError];
}


- (BOOL)openPath:(NSString *)path flags:(int)flags vfs:(NSString *)VFS error:(NSError **)outError {
    [self close];
    self.path = path;
    const char *zPath = [path cStringUsingEncoding:NSUTF8StringEncoding];
    const char *zVFS = [VFS cStringUsingEncoding:NSUTF8StringEncoding];
    int err = sqlite3_open_v2(zPath, &_sqlite3, flags, zVFS);
    if (err != SQLITE_OK) {
        NSError *error = [self errorWithSQL3ErrorCode:err];
        [self logError:error];
        SetError(outError, error);
        sqlite3_close(_sqlite3);
        _sqlite3 = NULL;
        return NO;
    }
    return YES;
}


- (BOOL)attachPath:(NSString *)path name:(NSString *)name error:(NSError **)outError {
    const char *zPath = [path cStringUsingEncoding:NSUTF8StringEncoding];
    const char *zName = [name cStringUsingEncoding:NSUTF8StringEncoding];
    char *zSQL = sqlite3_mprintf("ATTACH DATABASE %Q as \"%s\";", zPath, zName);
    NSString *SQL = @(zSQL);
    sqlite3_free(zSQL);
    
    _attaches[name] = path;
    
    NSError *error;
    if (![self executeSQL:SQL bindingKeyValues:nil withError:&error]) {
        [self logError:error];
        SetError(outError, error);
        return NO;
    }
    
    return YES;
}


- (void)close {
    if (_sqlite3) {
        if (sqlite3_close(_sqlite3) == SQLITE_OK) {
            _sqlite3 = NULL;
        }
    }
}


- (SQLPRStmt *)newStmt {
    return [[SQLPRStmt alloc] initWithDatabase:self];
}


- (SQLPRStmt *)newStmtWithSQL:(NSString *)SQL bindingKeyValues:(NSDictionary *)keyValues withError:(NSError **)outError {
    return [self newStmtWithSQL:SQL bindingKeyValues:keyValues tail:nil withError:outError];
}

- (SQLPRStmt *)newStmtWithSQL:(NSString *)SQL bindingKeyValues:(NSDictionary *)keyValues tail:(NSString **)tail withError:(NSError **)outError {
    SQLPRStmt *stmt = [[SQLPRStmt alloc] initWithDatabase:self];
    NSError *error;
    if (![stmt prepare:SQL remaining:nil withError:&error]) {
        [self logError:error];
        SetError(outError, error);
        return nil;
    }
    if (![stmt bindKeyValues:keyValues withError:&error]) {
        [self logError:error];
        SetError(outError, error);
        return nil;
    }
    return stmt;
}


- (SQLPRStmt *)newStmtWithSQL:(NSString *)SQL bindingValues:(NSArray *)values withError:(NSError **)outError {
    return [self newStmtWithSQL:SQL bindingValues:values tail:nil withError:outError];
}


- (SQLPRStmt *)newStmtWithSQL:(NSString *)SQL bindingValues:(NSArray *)values tail:(NSString **)tail withError:(NSError **)outError {
    SQLPRStmt *stmt = [[SQLPRStmt alloc] initWithDatabase:self];
    NSError *error;
    if (![stmt prepare:SQL remaining:nil withError:&error]) {
        [self logError:error];
        SetError(outError, error);
        return nil;
    }
    if (![stmt bindArray:values withError:&error]) {
        [self logError:error];
        SetError(outError, error);
        return nil;
    }
    return stmt;
}


- (SQLPRTransaction *)newTransactionWithLabel:(NSString *)label {
    return [[SQLPRTransaction alloc] initWithDatabase:self label:label];
}


typedef BOOL (^SQLPackRatBindBlock)(SQLPRStmt *statement, NSError **outError);


- (BOOL)executeSQL:(NSString *)SQL bindBlock:(SQLPackRatBindBlock)bindBlock withError:(NSError **)outError {
    NSError *error;
    SQLPRStmt *st = [self newStmt];
    NSString *current = SQL;
    for (;;) {
        NSString *rest;
        if (![st prepare:current remaining:&rest withError:&error]) {
            [self logError:error];
            SetError(outError, error);
            return NO;
        }
        
        if (![st haveStmt]) {
            [st closeWithError:&error];
            return YES;
        }
        
        if (bindBlock) {
            if (!bindBlock(st, &error)) {
                [self logError:error];
                SetError(outError, error);
                [st closeWithError:&error];
                return NO;
            }
        }
        BOOL ok = [st skipWithError:&error];
        if (!ok) {
            [self logError:error];
            SetError(outError, error);
            [st closeWithError:&error];
            return NO;
        }
        
        [st closeWithError:&error];
        
        current = rest;
    }
}


- (NSNumber *)changesFromSQL:(NSString *)SQL bindBlock:(SQLPackRatBindBlock)bindBlock withError:(NSError **)outError {
    NSError *error;
    SQLPRStmt *st = [self newStmt];
    NSString *current = SQL;
    NSInteger changes = 0;
    for (;;) {
        NSString *rest;
        if (![st prepare:current remaining:&rest withError:&error]) {
            [self logError:error];
            SetError(outError, error);
            return nil;
        }
        
        if (![st haveStmt]) {
            [st closeWithError:&error];
            return [NSNumber numberWithInteger:changes];
        }
        
        if (bindBlock) {
            if (!bindBlock(st, &error)) {
                [self logError:error];
                SetError(outError, error);
                [st closeWithError:&error];
                return nil;
            }
        }
        BOOL ok = [st skipWithError:&error];
        if (!ok) {
            [self logError:error];
            SetError(outError, error);
            [st closeWithError:&error];
            return nil;
        }
        changes += sqlite3_changes(_sqlite3);
        
        [st closeWithError:&error];
        
        current = rest;
    }
}


- (BOOL)executeSQL:(NSString *)SQL bindingKeyValues:(NSDictionary *)keyValues withError:(NSError **)outError {
    NSError *error;
    SQLPackRatBindBlock bind = ^BOOL (SQLPRStmt *stmt, NSError **outE) {
        NSError *e;
        if (![stmt bindKeyValues:keyValues withError:&e]) {
            if (outE) { *outE = e; }
            return NO;
        }
        return YES;
    };
    
    if (![self executeSQL:SQL bindBlock:bind withError:&error]) {
        
        [self logError:error];
        SetError(outError, error);
        return NO;
    }
    return YES;
}


- (void)runInBackground:(dispatch_block_t)background {
    @synchronized(self) {
        if (!_background) _background = dispatch_queue_create("SQLPRDatabase", 0);
        dispatch_async(_background, ^{
            background();
        });
    }
}


- (void)executeSQL:(NSString *)SQL bindingKeyValues:(NSDictionary *)bindings completion:(SQLPRExecuteCompletionBlock)completion {
    [self runInBackground:^{
        NSError *e;
        [self executeSQL:SQL bindingKeyValues:bindings withError:&e];
    }];
}


- (NSNumber *)changesFromSQL:(NSString *)SQL bindingKeyValues:(NSDictionary *)keyValues withError:(NSError **)outError {
    NSError *error;
    SQLPackRatBindBlock bind = ^BOOL (SQLPRStmt *stmt, NSError **outE) {
        NSError *e;
        if (![stmt bindKeyValues:keyValues withError:&e]) {
            if (outE) { *outE = e; }
            return NO;
        }
        return YES;
    };
    
    NSNumber *changes = [self changesFromSQL:SQL bindBlock:bind withError:&error];
    if (!changes) {
        
        [self logError:error];
        SetError(outError, error);
        return nil;
    }
    return changes;
}


- (NSNumber *)changesFromSQL:(NSString *)SQL bindingValues:(NSArray *)values withError:(NSError **)outError {
    NSError *error;
    SQLPackRatBindBlock bind = ^BOOL (SQLPRStmt *stmt, NSError **outE) {
        NSError *e;
        if (![stmt bindArray:values withError:&e]) {
            if (outE) { *outE = e; }
            return NO;
        }
        return YES;
    };
    
    NSNumber *changes = [self changesFromSQL:SQL bindBlock:bind withError:&error];
    if (!changes) {
        
        [self logError:error];
        SetError(outError, error);
        return nil;
    }
    return changes;
}


- (NSNumber *)insertUsingSQL:(NSString *)SQL bindingKeyValues:(NSDictionary *)keyValues withError:(NSError **)outError {
    NSError *error;
    SQLPackRatBindBlock bind = ^BOOL (SQLPRStmt *stmt, NSError **outE) {
        NSError *e;
        if (![stmt bindKeyValues:keyValues withError:&e]) {
            if (outE) { *outE = e; }
            return NO;
        }
        return YES;
    };
    
    if (![self executeSQL:SQL bindBlock:bind withError:&error]) {
        
        [self logError:error];
        SetError(outError, error);
        return nil;
    }
    
    sqlite3_int64 rowID = sqlite3_last_insert_rowid([self sqlite3]);
    return @(rowID);
}



- (NSNumber *)insertUsingSQL:(NSString *)SQL bindingValues:(NSArray *)values withError:(NSError **)outError {
    NSError *error;
    SQLPackRatBindBlock bind = ^BOOL (SQLPRStmt *stmt, NSError **outE) {
        NSError *e;
        if (![stmt bindArray:values withError:&e]) {
            if (outE) { *outE = e; }
            return NO;
        }
        return YES;
    };
    
    if (![self executeSQL:SQL bindBlock:bind withError:&error]) {
        
        [self logError:error];
        SetError(outError, error);
        return nil;
    }
    
    sqlite3_int64 rowID = sqlite3_last_insert_rowid([self sqlite3]);
    return @(rowID);
}



- (BOOL)executeSQLFromPath:(NSString *)path bindingKeyValues:(NSDictionary *)keyValues withError:(NSError **)outError {
    NSError *error;
    NSString *SQL = [NSString stringWithContentsOfFile:path encoding:NSUTF8StringEncoding error:&error];
    if (!SQL) {
        [self logError:error];
        SetError(outError, error);
        return NO;
    }
    BOOL ok = [self executeSQL:SQL bindingKeyValues:keyValues withError:&error];
    if (!ok) {
        [self logError:error];
        SetError(outError, error);
        return NO;
    }
    return YES;
}


- (BOOL)executeSQLNamed:(NSString *)name fromBundle:(NSBundle *)bundle bindingKeyValues:(NSDictionary *)keyValues withError:(NSError **)outError {
    NSError *error;
    NSString *SQLPath = [bundle pathForResource:name ofType:@"sql"];
    if (![self executeSQLFromPath:SQLPath bindingKeyValues:keyValues withError:&error]) {
        [self logError:error];
        SetError(outError, error);
        return NO;
    }
    return YES;
}


static void SelectorFuncGlue(sqlite3_context *context, int argC, sqlite3_value **argsV) {
    NSDictionary *userData = (__bridge id)sqlite3_user_data(context);
    NSValue *funcValue = [userData objectForKey:FuncSelectorKey];
    NSObject *target = [userData objectForKey:TargetKey];
    SEL func;
    [funcValue getValue:&func];
    NSUInteger argCount = argC;
    
    NSMethodSignature *sig = [target methodSignatureForSelector:func];
    NSInvocation *invocation = [NSInvocation invocationWithMethodSignature:sig];
    [invocation setTarget:target];
    [invocation setSelector:func];
    [invocation setArgument:&context atIndex:2];
    [invocation setArgument:&argCount atIndex:3];
    [invocation setArgument:&argsV atIndex:4];
    [invocation invoke];
}


static void SelectorStepGlue(sqlite3_context *context, int argC, sqlite3_value **argsV) {
    NSDictionary *userData = (__bridge id)sqlite3_user_data(context);
    NSValue *funcValue = [userData objectForKey:StepSelectorKey];
    NSObject *target = [userData objectForKey:TargetKey];
    SEL func;
    [funcValue getValue:&func];
    
    NSMethodSignature *sig = [target methodSignatureForSelector:func];
    NSInvocation *invocation = [NSInvocation invocationWithMethodSignature:sig];
    [invocation setTarget:target];
    [invocation setSelector:func];
    [invocation setArgument:&context atIndex:2];
    [invocation setArgument:&argC atIndex:3];
    [invocation setArgument:&argsV atIndex:4];
    [invocation invoke];
}


static void SelectorFinalGlue(sqlite3_context *context) {
    NSDictionary *userData = (__bridge id)sqlite3_user_data(context);
    NSValue *funcValue = [userData objectForKey:FinalSelectorKey];
    NSObject *target = [userData objectForKey:TargetKey];
    SEL func;
    [funcValue getValue:&func];
    
    NSMethodSignature *sig = [target methodSignatureForSelector:func];
    NSInvocation *invocation = [NSInvocation invocationWithMethodSignature:sig];
    [invocation setTarget:target];
    [invocation setSelector:func];
    [invocation setArgument:&context atIndex:2];
    [invocation invoke];
}


- (BOOL)addFunctionNamed:(NSString *)name argCount:(NSInteger)argCount target:(NSObject *)target func:(SEL)function step:(SEL)step final:(SEL)final withError:(NSError **)outError {
    NSString *sig = [NSString stringWithFormat:@"%@-%ld", name, fromNSInteger(argCount)];
    [_functions removeObjectForKey:sig];
    
    NSMutableDictionary *functions;
    if (function || step || final) {
        functions = [NSMutableDictionary dictionary];
        functions[TargetKey] = target;
        if (function) {
            functions[FuncSelectorKey] = [NSValue valueWithBytes:&function objCType:@encode(SEL)];
        }
        if (step) {
            functions[StepSelectorKey] = [NSValue valueWithBytes:&step objCType:@encode(SEL)];
        }
        if (final) {
            functions[FinalSelectorKey] = [NSValue valueWithBytes:&final objCType:@encode(SEL)];
        }
        _functions[sig] = functions;
    }
    
    const char *nameCStr = [name cStringUsingEncoding:NSUTF8StringEncoding];
    void *pApp = (__bridge void *)functions;
    void *xFunc = function ? SelectorFuncGlue : NULL;
    void *xStep = step ? SelectorStepGlue : NULL;
    void *xFinal = final ? SelectorFinalGlue : NULL;
    int err = sqlite3_create_function(_sqlite3, nameCStr, (int)argCount, SQLITE_UTF8, pApp, xFunc, xStep, xFinal);
    if (err != SQLITE_OK) {
        NSError *error = [self errorWithSQL3ErrorCode:err];
        [self logError:error];
        SetError(outError, error);
        return NO;
    }
    
    return YES;
}


static void BlockFuncGlue(sqlite3_context *context, int argC, sqlite3_value **argsV) {
    NSDictionary *userData = (__bridge id)sqlite3_user_data(context);
    SQLPRCustomFuncBlock func = userData[FuncBlockKey];
    if (func) {
        func(context, argC, argsV);
    }
}


static void BlockStepGlue(sqlite3_context *context, int argC, sqlite3_value **argsV) {
    NSDictionary *userData = (__bridge id)sqlite3_user_data(context);
    SQLPRCustomStepBlock step = userData[StepBlockKey];
    if (step) {
        step(context, argC, argsV);
    }
}


static void BlockFinalGlue(sqlite3_context *context) {
    NSDictionary *userData = (__bridge id)sqlite3_user_data(context);
    SQLPRCustomFinalBlock final = userData[FinalBlockKey];
    if (final) {
        final(context);
    }
}


- (BOOL)addFunctionNamed:(NSString *)name argCount:(NSInteger)argCount  func:(SQLPRCustomFuncBlock)function step:(SQLPRCustomStepBlock)step final:(SQLPRCustomFinalBlock)final withError:(NSError **)outError {
    NSString *sig = [NSString stringWithFormat:@"%@-%ld", name, fromNSInteger(argCount)];
    [_functions removeObjectForKey:sig];
    
    NSMutableDictionary *functions;
    if (function || step || final) {
        functions = [NSMutableDictionary dictionary];
        if (function) {
            functions[FuncBlockKey] = function;
        }
        if (step) {
            functions[StepBlockKey] = step;
        }
        if (final) {
            functions[FinalBlockKey] = final;
        }
        _functions[sig] = functions;
    }
    
    const char *nameCStr = [name cStringUsingEncoding:NSUTF8StringEncoding];
    void *pApp = (__bridge void *)functions;
    void *xFunc = function ? BlockFuncGlue : NULL;
    void *xStep = step ? BlockStepGlue : NULL;
    void *xFinal = final ? BlockFinalGlue : NULL;
    int err = sqlite3_create_function(_sqlite3, nameCStr, (int)argCount, SQLITE_UTF8, pApp, xFunc, xStep, xFinal);
    if (err != SQLITE_OK) {
        NSError *error = [self errorWithSQL3ErrorCode:err];
        [self logError:error];
        SetError(outError, error);
        return NO;
    }
    
    return YES;
}


- (BOOL)removeFunctionNamed:(NSString *)name argCount:(NSInteger)argCount withError:(NSError **)outError {
    const char *nameCStr = [name cStringUsingEncoding:NSUTF8StringEncoding];
    int err = sqlite3_create_function(_sqlite3, nameCStr, (int)argCount, SQLITE_UTF8, NULL, NULL, NULL, NULL);
    if (err != SQLITE_OK) {
        NSError *error = [self errorWithSQL3ErrorCode:err];
        [self logError:error];
        SetError(outError, error);
        return NO;
    }
    
    NSString *sig = [NSString stringWithFormat:@"%@-%ld", name, fromNSInteger(argCount)];
    [_functions removeObjectForKey:sig];
    
    return YES;
}


- (NSArray *)recordsFromSQL:(NSString *)SQL bindingValues:(NSArray *)values withError:(NSError **)outError {
    NSAssert(!(_refuseMainThread && [NSThread isMainThread]), @"Called from main thread");
    
    NSError *error;
    SQLPRStmt *stmt = [self newStmtWithSQL:SQL bindingValues:values withError:&error];
    if (!stmt) {
        [self logError:error];
        if (outError) *outError = error;
        return nil;
    }
    
    NSArray *contents = [stmt contentsWithError:&error];
    if (!contents) {
        [self logError:error];
        SetError(outError, error);
        [stmt closeWithError:NULL];
        return nil;
    }
    
    [stmt closeWithError:NULL];
    return contents;
}


- (NSArray *)recordsFromSQL:(NSString *)SQL bindingKeyValues:(NSDictionary *)keyValues withError:(NSError **)outError {
    NSAssert(!(_refuseMainThread && [NSThread isMainThread]), @"Called from main thread");
    
    NSError *error;
    SQLPRStmt *stmt = [self newStmtWithSQL:SQL bindingKeyValues:keyValues withError:&error];
    if (!stmt) {
        [self logError:error];
        if (outError) *outError = error;
        return nil;
    }
    
    NSArray *contents = [stmt contentsWithError:&error];
    if (!contents) {
        [self logError:error];
        SetError(outError, error);
        [stmt closeWithError:NULL];
        return nil;
    }
    
    [stmt closeWithError:NULL];
    return contents;
}


- (void)recordsFromSQL:(NSString *)SQL bindingValues:(NSArray *)values completion:(SQLPRSelectFirstCompletionBlock)completion {
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

- (void)recordsFromSQL:(NSString *)SQL
      bindingKeyValues:(NSDictionary *)keyValues
            completion:(SQLPRSelectFirstCompletionBlock)completion {
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


- (NSDictionary *)firstRecordFromSQL:(NSString *)SQL bindingValues:(NSArray *)values withError:(NSError **)outError {
    NSError *error;
    SQLPRStmt *stmt = [self newStmtWithSQL:SQL bindingValues:values withError:&error];
    if (!stmt) {
        [self logError:error];
        if (outError) *outError = error;
        return nil;
    }
    
    NSDictionary *record = [stmt nextRecord:&error];
    if (!record) {
        [self logError:error];
        if (outError) *outError = error;
        [stmt closeWithError:NULL];
        return nil;
    }
    
    [stmt closeWithError:NULL];
    return record;
}


- (NSDictionary *)firstRecordFromSQL:(NSString *)SQL bindingKeyValues:(NSDictionary *)keyValues withError:(NSError **)outError {
    NSError *error;
    SQLPRStmt *stmt = [self newStmtWithSQL:SQL bindingKeyValues:keyValues withError:&error];
    if (!stmt) {
        [self logError:error];
        if (outError) *outError = error;
        return nil;
    }
    
    NSDictionary *record = [stmt nextRecord:&error];
    if (!record) {
        [self logError:error];
        if (outError) *outError = error;
        [stmt closeWithError:NULL];
        return nil;
    }
    
    [stmt closeWithError:NULL];
    return record;
}


- (void)firstRecordFromSQL:(NSString *)SQL bindingValues:(NSArray *)values completion:(SQLPRSelectCompletionBlock)completion {
    
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


- (void)firstRecordFromSQL:(NSString *)SQL bindingKeyValues:(NSDictionary *)keyValues completion:(SQLPRSelectCompletionBlock)completion {
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


- (NSNumber *)insertOrReplace:(NSDictionary *)keyValues intoTable:(NSString *)table withError:(NSError **)error {
    
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


- (NSArray *)insertRecords:(NSArray *)records intoTable:(NSString *)table withError:(NSError **)error {
    NSError *e;
    NSMutableArray *IDs = [NSMutableArray array];
    for (NSDictionary *record in records) {
        NSNumber *ID = [self insertOrAbort:record intoTable:table withError:&e];
        if (!ID) {
            if (error) *error = e;
            return nil;
        }
        [IDs addObject:ID];
    }
    return [IDs copy];
}


- (void)insertOrReplace:(NSDictionary *)values intoTable:(NSString *)table completion:(SQLPRInsertCompletionBlock)completion {
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


- (NSNumber *)insertOrAbort:(NSDictionary *)keyValues intoTable:(NSString *)table withError:(NSError **)error {
    
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


- (void)insertOrAbort:(NSDictionary *)values intoTable:(NSString *)table completion:(SQLPRInsertCompletionBlock)completion {
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
    NSMutableString *str = [NSMutableString stringWithFormat:@"sqlite3 \"%@\"\n", _path];
    for (NSString *name in _attaches) {
        NSString *path = [_attaches objectForKey:name];
        [str appendFormat:@"ATTACH '%@' AS %@;\n", path, name];
    }
    return str;
}


- (BOOL)wrapInTransactionContext:(NSString *)context block:(SQLPRTransactionBlock)block withError:(NSError **)outError {
    NSError *error;
    
    SQLPRTransaction *transaction = _transactionsEnabled ? [self newTransactionWithLabel:context] : nil;
    if (transaction && ![transaction beginImmediateWithError:&error]) {
        SetError(outError, error);
        return NO;
    }
    
    BOOL ok = block(&error);
    if (!ok) {
        SetError(outError, error);
        [transaction rollbackWithError:&error];
        return NO;
    }
    
    if (transaction && ![transaction commitWithError:&error]) {
        SetError(outError, error);
        [transaction rollbackWithError:&error];
        return NO;
    }
    
    return YES;
}


- (BOOL)backupTo:(SQLPRDatabase *)destination withError:(NSError **)error {
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
    if (result != SQLITE_DONE) {
        NSError *e = [[destination class] errorWithCode:sqlite3_errcode(other) message:@(sqlite3_errmsg(other))];
        [destination logError:e];
        if (error) *error = e;
        sqlite3_backup_finish(backup);
        return NO;
    }
    
    result = sqlite3_backup_finish(backup);
    if (result != SQLITE_OK) {
        NSError *e = [[destination class] errorWithCode:sqlite3_errcode(other) message:@(sqlite3_errmsg(other))];
        [destination logError:e];
        if (error) *error = e;
        return NO;
    }
    
    return YES;
}


@end
