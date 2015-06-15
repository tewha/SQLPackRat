//
//  main.swift
//  sqljson
//
//  Created by Steven Fisher on 2015-06-04.
//  Copyright (c) 2015 Steven Fisher. All rights reserved.
//

import Foundation

enum Error:Int32 {
    case None = 0
    case DatabaseMissing
    case DatabaseOpen
    case BindRead
    case BindDeserialize
    case QueryMissing
    case QueryRead
    case QueryExec
    case InputRead
    case InputDeserialize
    case InputFormat
    case KeyMissing
    case OutputSerialize
    case OutputWrite
}

func + <K,V>(left: Dictionary<K,V>, right: Dictionary<K,V>)
    -> Dictionary<K,V>
{
    var map = Dictionary<K,V>()
    for (k, v) in left {
        map[k] = v
    }
    for (k, v) in right {
        map[k] = v
    }
    return map
}

func Fail(code:Error, message:String) {
    let data = message.stringByAppendingString("\n").dataUsingEncoding(NSUTF8StringEncoding)!
    NSFileHandle.fileHandleWithStandardError().writeData(data)
    exit(code.rawValue)
}

var e: NSError?

let parameters = NSUserDefaults.standardUserDefaults().dictionaryRepresentation()
let database = SQLPRDatabase()

let databasePathOptional = parameters["database"] as? String
if databasePathOptional == nil {
    Fail(Error.DatabaseMissing, "Error opening database: no path")
}
let databasePath = databasePathOptional!

if !database.openPath(databasePath.stringByExpandingTildeInPath, flags: SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, vfs: nil, error: &e) {
    Fail(Error.DatabaseOpen, String(format:"Error opening database: %@: %@", databasePath, e!.localizedDescription))
}

// Build bindings
var bindings = [String:AnyObject]()
let bindingsPath = parameters["bindings"] as? String
if let path = bindingsPath {
    let optionalData = NSData(contentsOfFile: path, options: nil, error: &e)
    if optionalData == nil {
        Fail(Error.BindRead, String(format:"Error reading bindings %@: %@", path, e!.localizedDescription))
    }
    let data = optionalData!
    let deserialized:AnyObject? = NSJSONSerialization.JSONObjectWithData(data, options: NSJSONReadingOptions.MutableContainers, error: &e)
    let bindingsFromFileOptional:[String:AnyObject]? = deserialized as? [String:AnyObject]
    if bindingsFromFileOptional == nil {
        Fail(Error.BindDeserialize, String(format:"Error deserializing bindings %@: %@", path, e!.localizedDescription))
    }
    let bindingsFromFile = bindingsFromFileOptional!
    bindings = bindings + bindingsFromFile
}

// Get the SQL to run.
var querySQL = parameters["query"] as? String
if querySQL == nil {
    let queryPathOptional = parameters["querypath"] as? String
    if queryPathOptional == nil {
        Fail(Error.QueryMissing, "Need -query or -querypath")
    }
    var path = queryPathOptional!
    querySQL = String(contentsOfFile:path.stringByExpandingTildeInPath, encoding:NSUTF8StringEncoding, error: &e)
    if querySQL == nil {
        Fail(Error.QueryRead, String(format:"Error reading -querypath %@: %@", path, e!.localizedDescription))
    }
}

// Run the SQL, capturing the resulting records.
let recordsOptional = database.recordsFromSQL(querySQL, bindingKeyValues: bindings, withError: &e)
if recordsOptional == nil {
    Fail(Error.QueryExec, String(format:"Error running query: %@", e!.localizedDescription))
}
let records = recordsOptional!

// What we're writing depends on what we're passed.
// If we're passed an -input, it's a dictionary with the records inserted as -key
// If we're passed only a -key, it's a dictionary with key:records
// If we're not passed an input or a key, it's an array.
let object:AnyObject
let inputPath = (parameters["input"] as? String) ?? (parameters["in"] as? String)
if let path = inputPath {
    let dataOptional = NSData(contentsOfFile: path.stringByExpandingTildeInPath, options: nil, error: &e)
    if dataOptional == nil {
        Fail(Error.InputRead, String(format:"Error reading -input %@: %@", path, e!.localizedDescription))
    }
    let data = dataOptional!
    
    let objectOptional:AnyObject? = NSJSONSerialization.JSONObjectWithData(data, options: NSJSONReadingOptions.MutableContainers, error: &e)
    if objectOptional == nil {
        Fail(Error.InputDeserialize, String(format:"Error deserializing %@: %@", path, e!.localizedDescription))
    }
    object = objectOptional!
    
    let keyOptional = parameters["key"] as? String
    if keyOptional == nil {
        Fail(Error.KeyMissing, String(format:"-key required if -input specified"))
    }
    let key = keyOptional!
    
    let dictionaryOptional = object as? NSMutableDictionary
    if dictionaryOptional == nil {
        Fail(Error.InputFormat, String(format:"-input %@ was not a dictionary: %@", path, e!.localizedDescription))
    }
    let dictionary = dictionaryOptional!
    dictionary[key] = records
    
} else {
    if let key = parameters["key"] as? String {
        object = [key:records]
    } else {
        object = records
    }
}

// Serialize the object to JSON.
let serializedOptional:NSData? = NSJSONSerialization.dataWithJSONObject(object, options: NSJSONWritingOptions.PrettyPrinted, error: &e)
if serializedOptional == nil {
    Fail(Error.OutputSerialize, String(format:"Error serializing output: %@", e!.localizedDescription))
}
let serialized = serializedOptional!

// Write out the result.
let outputPath = (parameters["output"] as? String) ?? (parameters["out"] as? String)
if let path = outputPath {
    // …to a file
    if !serialized.writeToFile(path.stringByExpandingTildeInPath, options:NSDataWritingOptions.DataWritingAtomic, error:&e) {
        Fail(Error.OutputWrite, String(format:"Error writing output %@: %@", path, e!.localizedDescription))
    }
} else {
    // …or to stdout
    println(NSString(data:serialized, encoding:NSUTF8StringEncoding))
}
