//
//  SQLPackRatTransaction.m
//  SQLPackRat
//
//  Created by Steven Fisher on 2011/05/03.
//  Copyright 2011 Steven Fisher. All rights reserved.
//

#import "SQLPRTransaction.h"

#import "SQLPRDatabase.h"


#ifndef DEBUG_TRANSACTIONS
#define DEBUG_TRANSACTIONS 0
#endif


@interface SQLPRTransaction ()
@property (nonatomic, readwrite, strong) NSString *label;
@property (nonatomic, readwrite, strong) NSError *lastError;
@property (nonatomic, readwrite, strong) SQLPRDatabase *database;
@property (nonatomic, readwrite, assign) BOOL transaction;
@end


static inline void SetError(NSError **error, NSError *e) {
    if (error) *error = e;
}


@implementation SQLPRTransaction


- (instancetype)initWithDatabase:(SQLPRDatabase *)database label:(NSString *)label {
    self = [super init];
    if (!self) {
        return nil;
    }
    _database = database;
    _label = label;
    return self;
}


- (instancetype)initWithDatabase:(SQLPRDatabase *)database label:(NSString *)label startMode:(SQLPackRatTransactionStartMode)startMode withError:(NSError *__autoreleasing *)error {
    self = [super init];
    if (!self) {
        return nil;
    }
    NSError *e;
    _database = database;
    _label = label;
    switch (startMode) {
        case SQLPackRatTransactionStartModeAutomaticallyLater:
            if (![self beginWithError:&e]) {
                if (error) *error = e;
                return nil;
            }
            break;
        case SQLPackRatTransactionStartModeAutomaticallyNow:
            if (![self beginImmediateWithError:&e]) {
                if (error) *error = e;
                return nil;
            }
            break;
        default:
            break;
    }
    return self;
}


- (void)dealloc {
    if (_transaction) {
#if DEBUG_TRANSACTIONS
        NSLog(@"Transaction not closed:%@", _label);
#endif
        NSError *error;
        [self rollbackWithError:&error];
        _transaction = NO;
    }
    
}


- (BOOL)beginImmediateWithError:(NSError **)outError {
    NSError *error;
#if DEBUG_TRANSACTIONS
    NSLog(@"beginImmediate:%@", _label);
#endif
    if (![_database executeSQL:@"BEGIN IMMEDIATE;" bindingKeyValues:nil withError:&error]) {
        self.lastError = error;
        SetError(outError, error);
        return NO;
    }
    _transaction = YES;
    self.lastError = nil;
    return YES;
}


- (BOOL)beginWithError:(NSError **)outError {
    NSError *error;
#if DEBUG_TRANSACTIONS
    NSLog(@"begin:%@", _label);
#endif
    if (![_database executeSQL:@"BEGIN;" bindingKeyValues:nil withError:&error]) {
        self.lastError = error;
        SetError(outError, error);
        return NO;
    }
    _transaction = YES;
    self.lastError = nil;
    return YES;
}


- (BOOL)commitWithError:(NSError **)outError {
    NSError *error;
#if DEBUG_TRANSACTIONS
    NSLog(@"end:%@", _label);
#endif
    if (![_database executeSQL:@"END;" bindingKeyValues:nil withError:&error]) {
        self.lastError = error;
        SetError(outError, error);
        return NO;
    }
    _transaction = NO;
    self.lastError = nil;
    return YES;
}


- (BOOL)rollbackWithError:(NSError **)outError {
    NSError *error;
#if DEBUG_TRANSACTIONS
    NSLog(@"rollback:%@", _label);
#endif
    if (![_database executeSQL:@"ROLLBACK;" bindingKeyValues:nil withError:&error]) {
        self.lastError = error;
        SetError(outError, error);
        return NO;
    }
    _transaction = NO;
    self.lastError = nil;
    return YES;
}


- (BOOL)isOpen {
    return _transaction;
}


- (NSString *)description {
    return [NSString stringWithFormat:@"<SQLPackRatTransaction:%@>{open = %@}", _label, _transaction ? @"YES" : @"NO"];
}


@end