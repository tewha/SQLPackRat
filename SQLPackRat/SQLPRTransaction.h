//
//  SQLPRTransaction.h
//  SQLPackRat
//
//  Created by Steven Fisher on 2011/05/03.
//  Copyright 2011 Steven Fisher. All rights reserved.
//

#import <Foundation/Foundation.h>

@class SQLPRDatabase;

typedef NS_ENUM(NSInteger, SQLPackRatTransactionStartMode) {
    SQLPackRatTransactionStartModeManually,
    SQLPackRatTransactionStartModeAutomaticallyLater,
    SQLPackRatTransactionStartModeAutomaticallyNow
};

/** A SQLPRTransaction represents a database transaction.
 
 Transactions must be explicitly started and committed. If a transaction is deallocated prior to being committed, it will be rolled back. */
@interface SQLPRTransaction : NSObject

- (instancetype)initWithDatabase:(SQLPRDatabase *)database label:(NSString *)label;

- (instancetype)initWithDatabase:(SQLPRDatabase *)database label:(NSString *)label startMode:(SQLPackRatTransactionStartMode)startMode withError:(NSError **)error;

@property (nonatomic, readonly, strong) NSError *lastError;

- (BOOL)beginImmediateWithError:(NSError **)outError;
- (BOOL)beginWithError:(NSError **)outError;
- (BOOL)commitWithError:(NSError **)outError;
- (BOOL)rollbackWithError:(NSError **)outError;
- (BOOL)isOpen;

@end
