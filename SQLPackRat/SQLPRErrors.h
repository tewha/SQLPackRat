//
//  SQLPRErrors.h
//  SQLPackRat
//
//  Created by Steven Fisher on 2015-06-12.
//  Copyright (c) 2015 Steven Fisher. All rights reserved.
//

#import <Foundation/Foundation.h>

extern NSString *SQLPRSQL3ErrorDomain;
extern NSString *SQLPRPackRatErrorDomain;

typedef NS_ENUM(NSInteger, SQLPackRatWrapperError) {
    SQLPRPackRatErrorSuccess = 0,
    SQLPRPackRatErrorNoRecords,
    SQLPRPackRatErrorUnsupportedType
};
