//
//  NSNumber+SQLPackRat.h
//  SQLPackRat
//
//  Created by Steven Fisher on 2012-04-12.
//  Copyright (c) 2012 Steven Fisher. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <sqlite3.h>

@interface NSNumber (SQLPackRat)

+ (instancetype)numberWithSqliteInt64:(sqlite3_int64)value;
- (instancetype)initWithSqliteInt64:(sqlite3_int64)value;
- (sqlite3_int64)sqliteInt64Value;

@end
