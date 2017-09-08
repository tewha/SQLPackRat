//
//  NSNumber+SQLPackRat.m
//  SQLPackRat
//
//  Created by Steven Fisher on 2012-04-12.
//  Copyright (c) 2012 Steven Fisher. All rights reserved.
//

#import "NSNumber+SQLPackRat.h"
#import <sqlite3.h>

@implementation NSNumber (SQLPackRat)


+ (instancetype)numberWithSqliteInt64:(int64_t)value {
    return [self numberWithLongLong:value];
}


- (instancetype)initWithSqliteInt64:(int64_t)value {
    self = [self initWithLongLong:value];
    if (self) {
        
    }
    return self;
}

- (int64_t)sqliteInt64Value {
    return [self longLongValue];
}

@end
