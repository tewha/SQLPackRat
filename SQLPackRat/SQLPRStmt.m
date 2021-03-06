//
//  SQLPRStmt.m
//  SQLPackRat
//
//  Created by Steven Fisher on 2011-04-29.
//  Copyright 2011 Steven Fisher. All rights reserved.
//

#import "SQLPRStmt.h"

#import "SQLPRErrors.h"
#import "SQLPRDatabase.h"

#import <sqlite3.h>

#if ((defined(__LP64__) && __LP64__) || (TARGET_OS_EMBEDDED && !TARGET_OS_IPHONE) || TARGET_OS_WIN32 || (defined(NS_BUILD_32_LIKE_64) && NS_BUILD_32_LIKE_64))
#   define INTS_ARE_64BIT 1
#else
#   define INTS_ARE_64BIT 0
#endif

typedef NS_ENUM(NSInteger, SQLRatPackObjectType) {
    SQLRatPackObjectTypeUnknown,
    SQLRatPackObjectTypeNull,
    SQLRatPackObjectTypeText,
    SQLRatPackObjectTypeInteger,
    SQLRatPackObjectTypeFloat,
    SQLRatPackObjectTypeBlob
};



@interface SQLPRDatabase ()
- (void)logError:(NSError *)error;
@end

@interface SQLPRStmt ()
@property (nonatomic, readwrite, strong) SQLPRDatabase *database;
@property (nonatomic, readwrite, assign) sqlite3_stmt *stmt;
@property (nonatomic, readwrite, strong) NSString *current;
@property (nonatomic, readwrite, assign) BOOL done;
@property (nonatomic, readwrite, assign) BOOL haveRow;
@end

static inline void SetError(NSError **error, NSError *e) {
    if (error) *error = e;
}


@implementation SQLPRStmt


- (void)logError:(NSError *)error {
    [self.database logError:error];
}


- (instancetype)initWithDatabase:(SQLPRDatabase *)database {
    self = [super init];
    if (!self) {
        return nil;
    }
    _database = database;
    return self;
}


- (void)dealloc {
    NSError *error;
#if defined(DEBUG) && DEBUG
    NSAssert(!_stmt, @"unclosed statement:%@", _current);
#endif
    [self closeWithError:&error];
    
}


- (NSError *)errorWithSQL3ErrorCode:(NSInteger)errorCode {
    const char *errMsg = sqlite3_errmsg([self.database sqlite3]);
    NSDictionary *userInfo = @{NSLocalizedDescriptionKey:@(errMsg), @"CurrentSQL":self.current ?: @""};
    NSError *error = [NSError errorWithDomain:SQLPRSQL3ErrorDomain code:errorCode userInfo:userInfo];
    return error;
}



- (BOOL)prepare:(NSString *)SQL remaining:(NSString **)outRemaining withError:(NSError **)outError {
    NSError *error;
    if (![self closeWithError:&error]) {
        [self logError:error];
        self.current = nil;
        SetError(outError, error);
        if (outRemaining) { *outRemaining = SQL; }
        return NO;
    }
    sqlite3 *sqlite3 = [self.database sqlite3];
    NSData *sqlData = [SQL dataUsingEncoding:NSUTF8StringEncoding];
    const char *head = [sqlData bytes];
    const char *tail = NULL;
    NSUInteger length = [sqlData length];
    sqlite3_stmt *stmt = NULL;
    int err = sqlite3_prepare_v2(sqlite3, head, (int)length, &stmt, &tail);
    self.stmt = stmt;
    NSUInteger consumed = tail ? ((intptr_t)tail - (intptr_t)head) : 0;
    self.current = consumed > 0 ? [[NSString alloc] initWithBytes:head length:consumed encoding:NSUTF8StringEncoding] : nil;
    if (err != SQLITE_OK) {
        error = [self errorWithSQL3ErrorCode:err];
        [self logError:error];
        SetError(outError, error);
        return NO;
    }

    if (outRemaining) {
        *outRemaining = [[NSString alloc] initWithBytes:tail length:length - consumed encoding:NSUTF8StringEncoding];
    }
    
    return YES;
}


- (BOOL)closeWithError:(NSError **)outError {
    sqlite3_finalize(self.stmt);
    /* Intentionally ignore sqlite3_finalize's error. Per sqlite documentation:
     
     If the most recent evaluation of statement S failed, then sqlite3_finalize(S) returns the appropriate error code or extended error code.
     
     We already handled that error; we don't want it again. */
    self.stmt = NULL;
    self.current = nil;
    return YES;
}


- (BOOL)resetWithError:(NSError **)outError {
    int err = sqlite3_reset(self.stmt);
    if (err != SQLITE_OK) {
        NSError *error = [self errorWithSQL3ErrorCode:err];
        SetError(outError, error);
        return NO;
    }
    return YES;
}


- (BOOL)clearBindingsWithError:(NSError **)outError {
    int err = sqlite3_clear_bindings(self.stmt);
    if (err != SQLITE_OK) {
        NSError *error = [self errorWithSQL3ErrorCode:err];
        SetError(outError, error);
        return NO;
    }
    return YES;
}


- (SQLRatPackObjectType)sqliteTypeOfNSObject:(id)value {
    if (value == [NSNull null]) {
        return SQLRatPackObjectTypeNull;
    } else if ([value isKindOfClass:[NSString class]]) {
        return SQLRatPackObjectTypeText;
    } else if ([value isKindOfClass:[NSNumber class]]) {
        NSNumber *number = (NSNumber *)value;
        const char *objCType = [number objCType];
        if (strlen(objCType) == 1) {
            static const char *doubleTypes = "dc";
            static const char *intTypes = "cislq";
            static const char *unsignedIntTypes = "CISLQ";
            if (strstr(doubleTypes, objCType) != NULL) {
                return SQLRatPackObjectTypeFloat;
            } else if (strstr(intTypes, objCType) != NULL) {
                return SQLRatPackObjectTypeInteger;
            } else if (strstr(unsignedIntTypes, objCType) != NULL) {
                return SQLRatPackObjectTypeInteger;
            }
        }
    } else if ([value isKindOfClass:[NSData class]]) {
        return SQLRatPackObjectTypeBlob;
    }
    return SQLRatPackObjectTypeUnknown;
}


- (BOOL)bind:(NSObject *)value toIndex:(NSInteger)binding withError:(NSError **)outError {
    BOOL handled = YES;
    int err = SQLITE_MISUSE;
    SQLRatPackObjectType type = [self sqliteTypeOfNSObject:value];
    switch (type) {
        case SQLRatPackObjectTypeNull:
            err = sqlite3_bind_null(self.stmt, (int)binding);
            break;
        case SQLRatPackObjectTypeText:{
            NSString *string = (NSString *)value;
            NSData *data = [string dataUsingEncoding:NSUTF8StringEncoding];
            err = sqlite3_bind_text(self.stmt, (int)binding, [data bytes], (int)[data length], SQLITE_TRANSIENT);
            break;
        }
        case SQLRatPackObjectTypeInteger:{
            NSNumber *number = (NSNumber *)value;
            err = sqlite3_bind_int64(self.stmt, (int)binding, [number longLongValue]);
            break;
        }
        case SQLRatPackObjectTypeFloat:{
            NSNumber *number = (NSNumber *)value;
            err = sqlite3_bind_double(self.stmt, (int)binding, [number doubleValue]);
            break;
        }
        case SQLRatPackObjectTypeBlob:{
            NSData *data = (NSData *)value;
            err = sqlite3_bind_blob(self.stmt, (int)binding, [data bytes], (int)[data length], SQLITE_TRANSIENT);
            break;
        }
        default:
            handled = NO;
    }
    if (!handled) {
        NSError *error = [NSError errorWithDomain:SQLPRPackRatErrorDomain code:SQLPRPackRatErrorUnsupportedType userInfo:@{NSLocalizedDescriptionKey:@"wrapper doesn't support type", @"Value":value, @"ValueClass":NSStringFromClass([value class])}];
        SetError(outError, error);
        return NO;
    }
    if (err != SQLITE_OK) {
        NSError *error = [self errorWithSQL3ErrorCode:err];
        SetError(outError, error);
        return NO;
    }
    return YES;
}


- (BOOL)bind:(NSObject *)value toName:(NSString *)name withError:(NSError **)outError {
    const char *bindName = [name cStringUsingEncoding:NSUTF8StringEncoding];
    int idx = sqlite3_bind_parameter_index(self.stmt, bindName);
    if (idx == 0) {
        return YES;
    }
    return [self bind:value toIndex:idx withError:outError];
}


- (BOOL)bindKeyValues:(NSDictionary *)keyValues withError:(NSError **)outError {
    NSError *error;
    for (NSString *key in [keyValues allKeys]) {
        NSObject *value = [keyValues objectForKey:key];
        if (![self bind:value toName:key withError:&error]) {
            [self logError:error];
            SetError(outError, error);
            return NO;
        }
    }
    return YES;
}


- (BOOL)bindArray:(NSArray *)values withError:(NSError **)outError {
    NSError *error;
    NSInteger bind = 1;
    for (NSObject *object in values) {
        if (![self bind:object toIndex:bind++ withError:&error]) {
            [self logError:error];
            SetError(outError, error);
            return NO;
        }
    }
    return YES;
}


- (BOOL)stepWithError:(NSError **)outError {
    int err = sqlite3_step(self.stmt);
    self.done = (err == SQLITE_DONE);
    self.haveRow = (err == SQLITE_ROW);
    if (err < 100) {
        NSError *error = [self errorWithSQL3ErrorCode:err];
        SetError(outError, error);
        return NO;
    }
    return YES;
}


- (BOOL)haveStmt {
    BOOL haveStmt = (self.stmt != NULL);
    return haveStmt;
}


- (BOOL)skipWithError:(NSError **)outError {
    for (;;) {
        NSError *error;
        if (![self stepWithError:&error]) {
            [self logError:error];
            SetError(outError, error);
            return NO;
        }
        if (self.done) {
            return YES;
        }
    }
}



- (NSInteger)numberOfColumns {
    NSInteger count = (NSInteger)sqlite3_column_count(self.stmt);
    return count;
}


- (NSString *)columnNameByIndex:(NSInteger)column {
    const char *text = (const char *)sqlite3_column_name(self.stmt, (int)column);
    NSString *str = text ? @(text) : nil;
    return str;
}


- (int)columnTypeByIndex:(NSInteger)column {
    return sqlite3_column_type(self.stmt, (int)column);
}



- (id<NSObject>)columnValueByIndex:(NSInteger)column {
    int type = [self columnTypeByIndex:column];
    id value;
    switch (type) {
        case SQLITE_INTEGER:
            value = @(sqlite3_column_int64(self.stmt, (int)column));
            break;
        case SQLITE_FLOAT:
            value = @(sqlite3_column_double(self.stmt, (int)column));
            break;
        case SQLITE_BLOB:{
            const void *blob = sqlite3_column_blob(self.stmt, (int)column);
            int bytes = sqlite3_column_bytes(self.stmt, (int)column);
            value = [NSData dataWithBytes:blob length:bytes];
            break;
        }
        case SQLITE_TEXT:{
            value = @((const char *)sqlite3_column_text(self.stmt, (int)column));
            break;
        }
        case SQLITE_NULL:
        default:{
            value = [NSNull null];
        }
    }
    return value;
}



- (NSString *)columnStringByIndex:(NSInteger)column {
    const char *text = (const char *)sqlite3_column_text(self.stmt, (int)column);
    NSString *str = text ? @(text) : nil;
    return str;
}



- (NSInteger)columnIntegerByIndex:(NSInteger)column {
    NSInteger result;
#if INTS_ARE_64BIT
    result = (NSInteger)sqlite3_column_int64(self.stmt, (int)column);
#else
    result = (NSInteger)sqlite3_column_int(self.stmt, (int)column);
#endif
    return result;
}



- (NSUInteger)columnUIntegerByIndex:(NSInteger)column {
    NSUInteger result;
#if INTS_ARE_64BIT
    result = (NSUInteger)sqlite3_column_int64(self.stmt, (int)column);
#else
    result = (NSUInteger)sqlite3_column_int(self.stmt, (int)column);
#endif
    return result;
}



- (NSString *)description {
    return [NSString stringWithFormat:@"SQLPackRatStmt:%@", self.current];
}



- (NSArray *)columns {
    NSMutableArray *result = [NSMutableArray array];
    NSInteger count = [self numberOfColumns];
    for (NSInteger column = 0; column < count; ++column) {
        [result addObject:[self columnNameByIndex:column]];
    }
    return [result copy];
}



- (NSArray *)row {
    NSMutableArray *result = [NSMutableArray array];
    NSInteger count = [self numberOfColumns];
    for (NSInteger column = 0; column < count; ++column) {
        [result addObject:[self columnValueByIndex:column]];
    }
    return [result copy];
}



- (NSDictionary *)rowWithError:(NSError **)outError {
    NSMutableDictionary *row = [[NSMutableDictionary alloc] init];
    NSInteger index = 0;
    for (NSString *name in self.columns) {
        id value = [self columnValueByIndex:index++];
        if (!value || value == [NSNull null]) continue;
        row[name] = value;
    }
    return [row copy];
}



- (NSArray *)contentsWithError:(NSError **)outError {
    NSError *error;
    if (![self stepWithError:&error]) {
        [self logError:error];
        SetError(outError, error);
        return nil;
    }
    
    NSInteger count = [self numberOfColumns];
    
    NSMutableArray *rows = [NSMutableArray array];
    while ([self haveRow]) {
        NSMutableDictionary *row = [[NSMutableDictionary alloc] init];
        for (NSInteger column = 0; column < count; ++column) {
            NSString *name = [self columnNameByIndex:column] ? : @"";
            id value = [self columnValueByIndex:column];
            if (!value || value == [NSNull null]) continue;
            row[name] = value;
        }
        
        [rows addObject:[NSDictionary dictionaryWithDictionary:row]];
        
        if (![self stepWithError:&error]) {
            [self logError:error];
            SetError(outError, error);
            return nil;
        }
    }
    
    return [NSArray arrayWithArray:rows];
}


- (NSDictionary *)nextRecord:(NSError **)outError {
    NSError *error;
    if (![self stepWithError:&error]) {
        [self logError:error];
        if (outError) *outError = error;
        return nil;
    }
    if (![self haveRow]) {
        NSDictionary *userInfo = @{NSLocalizedDescriptionKey:@"Read past end of table"};
        error = [NSError errorWithDomain:SQLPRSQL3ErrorDomain code:SQLITE_DONE userInfo:userInfo];
        [self logError:error];
        if (outError) *outError = error;
        return nil;
    }
    
    NSDictionary *record = [self rowWithError:&error];
    if (!record) {
        [self logError:error];
        if (outError) *outError = error;
        return nil;
    }
    
    return record;
}


- (NSUInteger)countByEnumeratingWithState:(NSFastEnumerationState *)state objects:(__unsafe_unretained id [])buffer count:(NSUInteger)len {
    NSError *error;
    
    
    state->mutationsPtr = (unsigned long *)&_stmt;
    state->itemsPtr = buffer;
    
    NSDictionary *__autoreleasing record = [self nextRecord:&error];
    if (!record) {
        [self logError:error];
        return 0;
    }
    
    buffer[0] = record;
    return 1;
}


@end
