//
//  SQLPackRatStmt.h
//  SQLPackRat
//
//  Created by Steven Fisher on 2011-04-29.
//  Copyright 2011 Steven Fisher. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <sqlite3.h>

@class SQLPackRatDatabase;

@interface SQLPackRatStmt :NSObject<NSFastEnumeration>

+ (instancetype)stmtWithDatabase:(SQLPackRatDatabase *)database;
- (instancetype)initWithDatabase:(SQLPackRatDatabase *)database;

- (BOOL)prepare:(NSString *)SQL remaining:(NSString **)outRemaining withError:(NSError **)outError;

- (BOOL)closeWithError:(NSError **)outError;

- (BOOL)resetWithError:(NSError **)outError;

- (BOOL)clearBindingsWithError:(NSError **)outError;

- (BOOL)bind:(NSObject *)value toIndex:(NSInteger)binding withError:(NSError **)outError;

- (BOOL)bindKeyValues:(NSDictionary *)keyValues withError:(NSError **)outError;

- (BOOL)bindArray:(NSArray *)values withError:(NSError **)outError;

- (BOOL)stepWithError:(NSError **)outError;

- (BOOL)skipWithError:(NSError **)outError;

- (NSInteger)numberOfColumns;

- (NSString *)columnNameByIndex:(NSInteger)column;

- (NSString *)columnStringByIndex:(NSInteger)column;

- (NSInteger)columnIntegerByIndex:(NSInteger)column;

- (NSUInteger)columnUIntegerByIndex:(NSInteger)column;

- (BOOL)haveStmt;

- (BOOL)haveRow;

- (BOOL)done;

- (NSArray *)columns;
- (NSArray *)row;

- (NSArray *)contentsWithError:(NSError **)outError;

- (NSDictionary *)nextRecord:(NSError **)outError;

@end
