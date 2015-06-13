//
//  SQLPackRatTransaction.h
//  SQLPackRat
//
//  Created by Steven Fisher on 2011/05/03.
//  Copyright 2011 Steven Fisher. All rights reserved.
//

#import <Foundation/Foundation.h>

@class SQLPackRatDatabase;

typedef NS_ENUM(NSInteger, SQLPackRatTransactionStartMode) {
    SQLPackRatTransactionStartModeManually,
    SQLPackRatTransactionStartModeAutomaticallyLater,
    SQLPackRatTransactionStartModeAutomaticallyNow
};

@interface SQLPackRatTransaction :NSObject

+ (instancetype)transactionWithDatabase:(SQLPackRatDatabase *)database label:(NSString *)label;

+ (instancetype)transactionWithDatabase:(SQLPackRatDatabase *)database label:(NSString *)label startMode:(SQLPackRatTransactionStartMode)startMode withError:(NSError **)error;

- (instancetype)initWithDatabase:(SQLPackRatDatabase *)database label:(NSString *)label;

- (instancetype)initWithDatabase:(SQLPackRatDatabase *)database label:(NSString *)label startMode:(SQLPackRatTransactionStartMode)startMode withError:(NSError **)error;

@property (nonatomic, readonly, strong) NSError *lastError;

- (BOOL)beginImmediateWithError:(NSError **)outError;
- (BOOL)beginWithError:(NSError **)outError;
- (BOOL)commitWithError:(NSError **)outError;
- (BOOL)rollbackWithError:(NSError **)outError;
- (BOOL)isOpen;

@end
