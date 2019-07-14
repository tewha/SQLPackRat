//
//  SQLPRTransaction.m
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


@implementation SQLPRTransaction {
}


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
    self.database = database;
    self.label = label;
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
    NSLog(@"beginImmediate:%@", self.label);
#endif
    if (![self.database executeSQL:@"BEGIN IMMEDIATE;" bindingKeyValues:nil withError:&error]) {
        self.lastError = error;
        SetError(outError, error);
        return NO;
    }
    self.transaction = YES;
    self.lastError = nil;
    return YES;
}


- (BOOL)beginWithError:(NSError **)outError {
    NSError *error;
#if DEBUG_TRANSACTIONS
    NSLog(@"begin:%@", self.label);
#endif
    if (![self.database executeSQL:@"BEGIN;" bindingKeyValues:nil withError:&error]) {
        self.lastError = error;
        SetError(outError, error);
        return NO;
    }
    self.transaction = YES;
    self.lastError = nil;
    return YES;
}


- (BOOL)commitWithError:(NSError **)outError {
    NSError *error;
#if DEBUG_TRANSACTIONS
    NSLog(@"end:%@", self.label);
#endif
    if (![self.database executeSQL:@"END;" bindingKeyValues:nil withError:&error]) {
        self.lastError = error;
        SetError(outError, error);
        return NO;
    }
    self.transaction = NO;
    self.lastError = nil;
    return YES;
}


- (BOOL)rollbackWithError:(NSError **)outError {
    NSError *error;
#if DEBUG_TRANSACTIONS
    NSLog(@"rollback:%@", self.label);
#endif
    if (![self.database executeSQL:@"ROLLBACK;" bindingKeyValues:nil withError:&error]) {
        self.lastError = error;
        SetError(outError, error);
        return NO;
    }
    self.transaction = NO;
    self.lastError = nil;
    return YES;
}


- (BOOL)isOpen {
    return self.transaction;
}


- (NSString *)description {
    return [NSString stringWithFormat:@"<SQLPackRatTransaction:%@>{open = %@}", self.label, self.transaction ? @"YES" : @"NO"];
}


@end
