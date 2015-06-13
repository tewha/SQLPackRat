//
//  NSString+SQLPackRat.h
//  SQLPackRat
//
//  Created by Steven Fisher on 2011-08-15.
//  Copyright 2011 Steven Fisher. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <sqlite3.h>

@interface NSString (SQLPackRat)

- (instancetype)initWithSQLFormat:(char *)format, ...;
+ (instancetype)stringWithSQLFormat:(char *)format, ...;
+ (instancetype)stringWithSqliteInt64: (sqlite3_int64)value;

@end
