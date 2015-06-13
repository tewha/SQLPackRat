//
//  CustomFunctionTests.m
//  SQLPackRat
//
//  Created by Steven Fisher on 2015-06-12.
//  Copyright (c) 2015 Steven Fisher. All rights reserved.
//

#import <XCTest/XCTest.h>
#import <Foundation/Foundation.h>
#import "SQLPackRat.h"

@interface CustomFunctionTests : XCTestCase

@end

@implementation CustomFunctionTests

- (SQLPackRatDatabase *)open:(NSError **)error {
    NSError *e;
    NSString *databasePath = [NSTemporaryDirectory() stringByAppendingString:[NSUUID UUID].UUIDString];
    SQLPackRatDatabase *database = [[SQLPackRatDatabase alloc] initWithPath:databasePath flags:SQLITE_OPEN_CREATE | SQLITE_OPEN_DELETEONCLOSE | SQLITE_OPEN_READWRITE vfs:nil error:&e];
    if (!database) {
        if (error) *error = e;
        return nil;
    }
    
    return database;
}

- (BOOL)buildValuesTable:(NSArray *)values inDatabase:(SQLPackRatDatabase *)database error:(NSError **)error {
    NSError *e;
    if (![database executeSQL:@"CREATE TABLE \"Values\"(\"Value\" INTEGER);" bindingKeyValues:nil withError:&e]) {
        if (error) *error = e;
        return NO;
    }
    
    for (id value in values) {
        if (![database insertOrAbort:@{@"Value":value} intoTable:@"Values" withError:&e]) {
            if (error) *error = e;
            return NO;
        }
    }
    
    return YES;
}


- (void)setUp {
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
}

- (void)tearDown {
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}

- (void)testAggregateBlock {
    NSError *e;
    SQLPackRatDatabase *database = [self open:&e];
    XCTAssertNotNil(database, @"Error opening database: %@", e);
    
    BOOL buildTablesOK = [self buildValuesTable:@[@1, @2, @3] inDatabase:database error:&e];
    XCTAssertTrue(buildTablesOK, @"Error building table: %@", e);
    
    BOOL success = [database createFunctionNamed:@"mySum" argCount:1 func:nil step:^(sqlite3_context *context, int argC, sqlite3_value **argsV) {
        sqlite3_int64 *sumPtr = (sqlite3_int64 *)sqlite3_aggregate_context(context, sizeof(sqlite3_int64));
        int64_t this = sqlite3_value_int64(argsV[0]);
        *sumPtr += this;
    } final:^(sqlite3_context *context) {
        sqlite3_int64 *sumPtr = (sqlite3_int64 *)sqlite3_aggregate_context(context, sizeof(sqlite3_int64));
        sqlite3_result_int64(context, *sumPtr);
    } withError:&e];
    XCTAssertTrue(success, @"Failed to register function: %@", e);
    
    NSDictionary *result = [database firstRecordFromSQL:@"SELECT mySum(Value) AS sum FROM \"Values\";" bindingKeyValues:nil withError:&e];
    XCTAssertNotNil(result, @"Failed to run query: %@", e);
    
    sqlite3_int64 sum = [result[@"sum"] longLongValue];
    XCTAssertEqual(sum, 6, @"Sum wrong.");
}

- (void)testExample {
    // This is an example of a functional test case.
    XCTAssert(YES, @"Pass");
}

- (void)testPerformanceExample {
    // This is an example of a performance test case.
    [self measureBlock:^{
        // Put the code you want to measure the time of here.
    }];
}

@end
