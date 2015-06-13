//
//  NSString+SQLPackRat.m
//  SQLPackRat
//
//  Created by Steven Fisher on 2011-08-15.
//  Copyright 2011 Steven Fisher. All rights reserved.
//

#import "NSString+SQLPackRat.h"

#import <Foundation/Foundation.h>
#import "sqlite3.h"

@implementation NSString (SQLPackRat)

- (instancetype)initWithSQLFormat:(char *)format, ... {
    va_list ap;
    va_start(ap, format);
    char *m = sqlite3_vmprintf(format, ap);
    self = [self initWithCString:m encoding:NSUTF8StringEncoding];
    sqlite3_free(m);
    va_end(ap);
    return self;
}

+ (instancetype)stringWithSQLFormat:(char *)format, ... {
    va_list ap;
    va_start(ap, format);
    char *m = sqlite3_vmprintf(format, ap);
    NSString *str = @(m);
    sqlite3_free(m);
    va_end(ap);
    return str;
}

+ (instancetype)stringWithSqliteInt64:(sqlite3_int64)value {
    return [NSString stringWithFormat:@"%lld", value];
}

@end
