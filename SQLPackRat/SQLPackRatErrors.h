//
//  SQLPackRatErrors.h
//  SQLPackRat
//
//  Created by Steven Fisher on 2015-06-12.
//  Copyright (c) 2015 Steven Fisher. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <sqlite3.h>

extern NSString *SQLPackRatSQL3ErrorDomain;
extern NSString *SQLPackRatWrapperErrorDomain;

typedef NS_ENUM(NSInteger, SQLPackRatWrapperError) {
    SQLPackRatWrapperErrorSwuccess = 0,
    SQLPackRatWrapperErrorNoRecords
};

typedef NS_ENUM(NSInteger, SQLPackRatSQL3Error) {
    SQLPackRatSQL3ErrorOK = SQLITE_OK,
    SQLPackRatSQL3ErrorError = SQLITE_ERROR,
    SQLPackRatSQL3ErrorInternal = SQLITE_INTERNAL,
    SQLPackRatSQL3ErrorPermission = SQLITE_PERM,
    SQLPackRatSQL3ErrorAbort = SQLITE_ABORT,
    SQLPackRatSQL3ErrorBusy = SQLITE_BUSY,
    SQLPackRatSQL3ErrorLocked = SQLITE_LOCKED,
    SQLPackRatSQL3ErrorNoMemory = SQLITE_NOMEM,
    SQLPackRatSQL3ErrorReadOnly = SQLITE_READONLY,
    SQLPackRatSQL3ErrorInterrupt = SQLITE_INTERRUPT,
    SQLPackRatSQL3ErrorIOError = SQLITE_IOERR,
    SQLPackRatSQL3ErrorCorrupt = SQLITE_CORRUPT,
    SQLPackRatSQL3ErrorNotFound = SQLITE_NOTFOUND,
    SQLPackRatSQL3ErrorFull = SQLITE_FULL,
    SQLPackRatSQL3ErrorCantOpen = SQLITE_CANTOPEN,
    SQLPackRatSQL3ErrorProtocol = SQLITE_PROTOCOL,
    SQLPackRatSQL3ErrorEmpty = SQLITE_EMPTY,
    SQLPackRatSQL3ErrorSchema = SQLITE_SCHEMA,
    SQLPackRatSQL3ErrorTooBig = SQLITE_TOOBIG,
    SQLPackRatSQL3ErrorConstraint = SQLITE_CONSTRAINT,
    SQLPackRatSQL3ErrorMismatch = SQLITE_MISMATCH,
    SQLPackRatSQL3ErrorMisuse = SQLITE_MISUSE,
    SQLPackRatSQL3ErrorNoLFS = SQLITE_NOLFS,
    SQLPackRatSQL3ErrorAuth = SQLITE_AUTH,
    SQLPackRatSQL3ErrorFormat = SQLITE_FORMAT,
    SQLPackRatSQL3ErrorRange = SQLITE_RANGE,
    SQLPackRatSQL3ErrorNotADB = SQLITE_NOTADB,
    SQLPackRatSQL3ErrorNotice = SQLITE_NOTICE,
    SQLPackRatSQL3ErrorWarning = SQLITE_WARNING,
    SQLPackRatSQL3ErrorRow = SQLITE_ROW,
    SQLPackRatSQL3ErrorDone = SQLITE_DONE
};