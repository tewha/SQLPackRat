//
//  SQLPackRat.h
//  SQLPackRat
//
//  Created by Steven Fisher on 2011/05/03.
//  Copyright 2011 Steven Fisher. All rights reserved.
//

#if defined(TARGET_OS_PHONE) && TARGET_OS_PHONE
#   if defined(__IPHONE_OS_VERSION_MIN_REQUIRED) && (__IPHONE_OS_VERSION_MIN_REQUIRED < 60000)
#       warning "Must be built for iOS SDK 6.0 and later."
#   endif
#endif

#import <sqlite3.h>
#import "SQLPRDatabase.h"
#import "SQLPRErrors.h"
#import "SQLPRTransaction.h"
#import "SQLPRStmt.h"
