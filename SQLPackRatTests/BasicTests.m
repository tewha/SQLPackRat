//
//  BasicTests.m
//  SQLPackRat
//
//  Created by Steven Fisher on 2015-06-15.
//  Copyright (c) 2015 Steven Fisher. All rights reserved.
//

//#import <UIKit/UIKit.h>
#import <XCTest/XCTest.h>
#import "SQLPackRat.h"

@interface BasicTests : XCTestCase
@property (strong, nonatomic) SQLPRDatabase *database;
@end

@implementation BasicTests

- (void)setUp {
    [super setUp];
    
    NSError *e;
    NSString *baseDirectory = [NSSearchPathForDirectoriesInDomains(NSLibraryDirectory, NSUserDomainMask, YES)[0] stringByAppendingPathComponent:@"net.tewha.sqlpackrat.tests"];
    
    [[NSFileManager defaultManager] createDirectoryAtPath:baseDirectory withIntermediateDirectories:YES attributes:nil error:&e];
    
    NSString *validPath = [baseDirectory stringByAppendingPathComponent:@"test.db"];
    self.database = [[SQLPRDatabase alloc] initWithPath:validPath flags:(SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_DELETEONCLOSE) vfs:nil error:&e];
    XCTAssertNotNil(self.database, @"No database returned.");
    
}


- (void)tearDown {
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}


- (void)testInvalidStatement {
    NSError *e;
    BOOL ok = [self.database executeSQL:@"CREATE TABLE INDEX(A);" bindingKeyValues:nil withError:&e];
    XCTAssertFalse(ok, @"Expected failure");
}


- (void)testInsert {
    NSMutableArray *expected = [NSMutableArray array];
    int value = 1;
    
    @autoreleasepool {
        NSError *e;
        BOOL ok = [self.database executeSQL:@"DROP TABLE IF EXISTS InsertTest; CREATE TABLE InsertTest(B);" bindingKeyValues:nil withError:&e];
        XCTAssertTrue(ok, @"Unexpected failure: %@", e);
    }
    
    @autoreleasepool {
        NSError *e;
        NSArray *usingInsertUsingSQL = @[@(value+0), @(value+1), @(value+2)];
        value += 3;
        for (NSNumber *value in usingInsertUsingSQL) {
            NSNumber *row = [self.database insertUsingSQL:@"INSERT INTO InsertTest(B) VALUES(:B);" bindingKeyValues:@{@":B":value} withError:&e];
            XCTAssertNotNil(row, @"Unexpected failure: %@", e);
        }
        [expected addObjectsFromArray:usingInsertUsingSQL];
    }
    
    @autoreleasepool {
        NSError *e;
        NSArray *usingInsertRecord = @[@(value+0), @(value+1), @(value+2)];
        value += 3;
        for (NSNumber *value in usingInsertRecord) {
            NSNumber *row = [self.database insertOrAbort:@{@"B":value} intoTable:@"InsertTest" withError:&e];
            XCTAssertNotNil(row, @"Unexpected failure: %@", e);
        }
        [expected addObjectsFromArray:usingInsertRecord];
    }
    
    @autoreleasepool {
        NSError *e;
        NSArray *usingInsertRecords = @[@{@"B":@(value+0)}, @{@"B":@(value+1)}, @{@"B":@(value+2)}];
        value += 3;
        NSArray *rows = [self.database insertRecords:usingInsertRecords intoTable:@"InsertTest" withError:&e];
        XCTAssertNotNil(rows, @"Unexpected failure: %@", e);
        for (NSDictionary *record in usingInsertRecords) {
            [expected addObject:record[@"B"]];
        }
    }
    
    @autoreleasepool {
        NSError *e;
        NSArray *records = [self.database recordsFromSQL:@"SELECT * FROM InsertTest ORDER BY B;" bindingKeyValues:nil withError:&e];
        XCTAssertNotNil(records, @"Unexpected failure: %@", e);
        NSMutableArray *actual = [NSMutableArray array];
        for (NSDictionary *record in records) {
            [actual addObject:record[@"B"]];
        }
        XCTAssertEqualObjects(actual, expected);
    }
    
    @autoreleasepool {
        NSError *e;
        SQLPRStmt *statement = [self.database newStmtWithSQL:@"SELECT * FROM InsertTest ORDER BY B;" bindingKeyValues:nil tail:nil withError:&e];
        NSMutableArray *actual = [NSMutableArray array];
        for (NSDictionary *record in statement) {
            [actual addObject:record[@"B"]];
        }
        XCTAssertEqualObjects(actual, expected);
        [statement closeWithError:nil];
    }
}

@end
