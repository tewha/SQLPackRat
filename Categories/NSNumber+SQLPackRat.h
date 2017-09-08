//
//  NSNumber+SQLPackRat.h
//  SQLPackRat
//
//  Created by Steven Fisher on 2012-04-12.
//  Copyright (c) 2012 Steven Fisher. All rights reserved.
//

#import <Foundation/Foundation.h>

@interface NSNumber (SQLPackRat)

+ (instancetype)numberWithSqliteInt64:(int64_t)value;
- (instancetype)initWithSqliteInt64:(int64_t)value;
- (int64_t)sqliteInt64Value;

@end
