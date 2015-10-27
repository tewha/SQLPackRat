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

@noreturn func Fail(code:Error, message:String) {
    let data = message.stringByAppendingString("\n").dataUsingEncoding(NSUTF8StringEncoding)!
    NSFileHandle.fileHandleWithStandardError().writeData(data)
    exit(code.rawValue)
}

let parameters = NSUserDefaults.standardUserDefaults().dictionaryRepresentation()

guard let databasePath = (parameters["database"] as? String) else {
    Fail(Error.DatabaseMissing, message: "Error opening database: no path")
}

let database:SQLPRDatabase;
do {
    database = try SQLPRDatabase(path: databasePath, flags: SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, vfs: nil);
} catch var error as NSError {
    Fail(Error.DatabaseOpen, message: String(format:"Error opening database: %@: %@", databasePath, error.localizedDescription))
}

// Build bindings
var bindings = [String:AnyObject]()
let bindingsPath = parameters["bindings"] as? String
if let path = bindingsPath {
    let data: NSData
    do {
        data = try NSData(contentsOfFile: path, options: [])
    } catch var error as NSError {
        Fail(Error.BindRead, message: String(format:"Error reading bindings %@: %@", path, error.localizedDescription))
    }
    let bindingsFromFile:Dictionary<String, AnyObject>
    do {
        bindingsFromFile = try NSJSONSerialization.JSONObjectWithData(data, options: NSJSONReadingOptions.MutableContainers) as! Dictionary<String, AnyObject>
    } catch var error as NSError {
        Fail(Error.BindDeserialize, message: String(format:"Error deserializing bindings %@: %@", path, error.localizedDescription))
    }
    bindings = bindings + bindingsFromFile
}

// Get the SQL to run.
let SQL:String
if let query = parameters["query"] as? String {
    SQL = query;
} else {
    guard let queryPath = parameters["querypath"] as? String else {
        Fail(Error.QueryMissing, message: "Need -query or -querypath")
    }
    do {
        SQL = try String(contentsOfFile:(queryPath as NSString).stringByExpandingTildeInPath, encoding:NSUTF8StringEncoding)
    } catch var error as NSError {
        Fail(Error.QueryRead, message: String(format:"Error reading -querypath %@: %@", queryPath, error.localizedDescription))
    }
}

// Run the SQL, capturing the resulting records.
let records:Array<AnyObject>
do {
    records = try database.recordsFromSQL(SQL, bindingKeyValues: bindings);
} catch var error as NSError {
    Fail(Error.QueryExec, message: String(format:"Error running query: %@", error.localizedDescription))
}

// What we're writing depends on what we're passed.
// If we're passed an -input, it's a dictionary with the records inserted as -key
// If we're passed only a -key, it's a dictionary with key:records
// If we're not passed an input or a key, it's an array.
let object:AnyObject
if let path = (parameters["input"] as? String) ?? (parameters["in"] as? String) {
    let dataOptional: NSData?
    do {
        dataOptional = try NSData(contentsOfFile: (path as NSString).stringByExpandingTildeInPath, options: [])
    } catch var error as NSError {
        Fail(Error.InputRead, message: String(format:"Error reading -input %@: %@", path, error.localizedDescription))
    }
    let data = dataOptional!
    
    do {
        object = try NSJSONSerialization.JSONObjectWithData(data, options: NSJSONReadingOptions.MutableContainers)
    } catch var error as NSError {
        Fail(Error.InputDeserialize, message: String(format:"Error deserializing %@: %@", path, error.localizedDescription))
    }
    
    guard let key = parameters["key"] as? String else {
        Fail(Error.KeyMissing, message: String(format:"-key required if -input specified"))
    }
    
    guard let dictionary = object as? NSMutableDictionary else {
        Fail(Error.InputFormat, message: String(format:"-input %@ was not a dictionary", path))
    }
    dictionary[key] = records
    
} else {
    if let key = parameters["key"] as? String {
        object = [key:records]
    } else {
        object = records
    }
}

// Serialize the object to JSON.
let JSON:String;
do {
    let serialized = try NSJSONSerialization.dataWithJSONObject(object, options: NSJSONWritingOptions.PrettyPrinted)
    guard let string = String(data:serialized, encoding:NSUTF8StringEncoding) else {
        Fail(Error.OutputWrite, message: String(format:"Error converting output to string"))
    }
    JSON = string;
} catch var error as NSError {
    Fail(Error.OutputSerialize, message: String(format:"Error serializing output: %@", error.localizedDescription))
}


// Write out the result.
let outputPath = (parameters["output"] as? String) ?? (parameters["out"] as? String)
if let path = outputPath {
    // …to a file
    do {
        try JSON.writeToFile((path as NSString).stringByExpandingTildeInPath, atomically:true, encoding: NSUTF8StringEncoding)
    } catch var error as NSError {
        Fail(Error.OutputWrite, message: String(format:"Error writing output %@: %@", path, error.localizedDescription))
    }
} else {
    // …or to stdout
    print(JSON)
}
