//
//  SQLPackRatTransaction.m
//  SQLPackRat
//
//  Created by Steven Fisher on 2011/05/03.
//  Copyright 2011 Steven Fisher. All rights reserved.
//

#import "SQLPackRatTransaction.h"

#import "SQLPackRatDatabase.h"


#ifndef DEBUG_TRANSACTIONS
#define DEBUG_TRANSACTIONS 0
#endif


@interface SQLPackRatTransaction()
@property (nonatomic, readwrite, strong) NSString *label;
@property (nonatomic, readwrite, strong) NSError *lastError;
@property (nonatomic, readwrite, strong) SQLPackRatDatabase *database;
@property (nonatomic, readwrite, assign) BOOL transaction;
@end


@implementation SQLPackRatTransaction


+ (instancetype)transactionWithDatabase: (SQLPackRatDatabase *)database
                                  label: (NSString *)label {
    return [[self alloc] initWithDatabase: database
                                    label: label];
}


+ (instancetype)transactionWithDatabase: (SQLPackRatDatabase *)database
                                  label: (NSString *)label
                              startMode: (SQLPackRatTransactionStartMode)startMode
                              withError: (NSError **)error {
    return [[self alloc] initWithDatabase: database
                                    label: label
                                startMode: startMode
                                withError: error];
}


- (instancetype)initWithDatabase: (SQLPackRatDatabase *)database
                           label: (NSString *)label {
    if (( self = [super init] )) {
        _database = database;
        _label = label;
    }
    return self;
}


- (instancetype)initWithDatabase: (SQLPackRatDatabase *)database
                           label: (NSString *)label
                       startMode: (SQLPackRatTransactionStartMode)startMode
                       withError: (NSError *__autoreleasing *)error {
    if (( self = [super init] )) {
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
    }
    return self;
}


- (void)dealloc {
    if ( _transaction ) {
#if DEBUG_TRANSACTIONS
        NSLog( @"Transaction not closed: %@", _label );
#endif
        NSError *error;
        [self rollbackWithError: &error];
        _transaction = NO;
    }
    
}


- (BOOL)beginImmediateWithError: (NSError **)outError {
    NSError *error;
#if DEBUG_TRANSACTIONS
    NSLog( @"beginImmediate: %@", _label );
#endif
    if ( ![_database executeSQL: @"BEGIN IMMEDIATE;"
               bindingKeyValues: nil
                      withError: &error] ) {
        self.lastError = error;
        if ( outError ) { *outError = error; }
        return NO;
    }
    _transaction = YES;
    self.lastError = nil;
    return YES;
}


- (BOOL)beginWithError: (NSError **)outError {
    NSError *error;
#if DEBUG_TRANSACTIONS
    NSLog( @"begin: %@", _label );
#endif
    if ( ![_database executeSQL: @"BEGIN;"
               bindingKeyValues: nil
                      withError: &error] ) {
        self.lastError = error;
        if ( outError ) { *outError = error; }
        return NO;
    }
    _transaction = YES;
    self.lastError = nil;
    return YES;
}


- (BOOL)commitWithError: (NSError **)outError {
    NSError *error;
#if DEBUG_TRANSACTIONS
    NSLog( @"end: %@", _label );
#endif
    if ( ![_database executeSQL: @"END;"
               bindingKeyValues: nil
                      withError: &error] ) {
        self.lastError = error;
        if ( outError ) { *outError = error; }
        return NO;
    }
    _transaction = NO;
    self.lastError = nil;
    return YES;
}


- (BOOL)rollbackWithError: (NSError **)outError {
    NSError *error;
#if DEBUG_TRANSACTIONS
    NSLog( @"rollback: %@", _label );
#endif
    if ( ![_database executeSQL: @"ROLLBACK;"
               bindingKeyValues: nil
                      withError: &error] ) {
        self.lastError = error;
        if ( outError ) { *outError = error; }
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
    return [NSString stringWithFormat: @"<SQLPackRatTransaction: %@>{open = %@}", _label, _transaction ? @"YES" : @"NO"];
}


@end
