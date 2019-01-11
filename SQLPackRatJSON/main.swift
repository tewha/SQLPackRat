//
//  main.swift
//  sqljson
//
//  Created by Steven Fisher on 2015-06-04.
//  Copyright (c) 2015 Steven Fisher. All rights reserved.
//

import Foundation

enum Error:Int32 {
    case none = 0
    case databaseMissing
    case databaseOpen
    case bindRead
    case bindDeserialize
    case queryMissing
    case queryRead
    case queryExec
    case inputRead
    case inputDeserialize
    case inputFormat
    case keyMissing
    case outputSerialize
    case outputWrite
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

func Fail(_ code:Error, message:String) -> Never  {
    let data = (message + "\n").data(using: String.Encoding.utf8)!
    FileHandle.standardError.write(data)
    exit(code.rawValue)
}

let parameters = UserDefaults.standard.dictionaryRepresentation()

guard let databasePath = (parameters["database"] as? String) else {
    Fail(Error.databaseMissing, message: "Error opening database: no path")
}

let database:SQLPRDatabase;
do {
    database = try SQLPRDatabase(path: databasePath, flags: SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, vfs: nil);
} catch let error as NSError {
    Fail(Error.databaseOpen, message: String(format:"Error opening database: %@: %@", databasePath, error.localizedDescription))
}

// Build bindings
var bindings = [String:AnyObject]()
let bindingsPath = parameters["bindings"] as? String
if let path = bindingsPath {
    let data: Data
    do {
        data = try Data(contentsOf: URL(fileURLWithPath: path), options: [])
    } catch let error as NSError {
        Fail(Error.bindRead, message: String(format:"Error reading bindings %@: %@", path, error.localizedDescription))
    }
    let bindingsFromFile:Dictionary<String, AnyObject>
    do {
        bindingsFromFile = try JSONSerialization.jsonObject(with: data, options: JSONSerialization.ReadingOptions.mutableContainers) as! Dictionary<String, AnyObject>
    } catch let error as NSError {
        Fail(Error.bindDeserialize, message: String(format:"Error deserializing bindings %@: %@", path, error.localizedDescription))
    }
    bindings = bindings + bindingsFromFile
}

// Get the SQL to run.
let SQL:String
if let query = parameters["query"] as? String {
    SQL = query;
} else {
    guard let queryPath = parameters["querypath"] as? String else {
        Fail(Error.queryMissing, message: "Need -query or -querypath")
    }
    do {
        SQL = try String(contentsOfFile:(queryPath as NSString).expandingTildeInPath, encoding:String.Encoding.utf8)
    } catch let error as NSError {
        Fail(Error.queryRead, message: String(format:"Error reading -querypath %@: %@", queryPath, error.localizedDescription))
    }
}

// Run the SQL, capturing the resulting records.
let records:Array<AnyObject>
do {
    records = try database.records(fromSQL: SQL, bindingKeyValues: bindings) as Array<AnyObject>;
} catch let error as NSError {
    Fail(Error.queryExec, message: String(format:"Error running query: %@", error.localizedDescription))
}

// What we're writing depends on what we're passed.
// If we're passed an -input, it's a dictionary with the records inserted as -key
// If we're passed only a -key, it's a dictionary with key:records
// If we're not passed an input or a key, it's an array.
let object:Any
if let path = (parameters["input"] as? String) ?? (parameters["in"] as? String) {
    let dataOptional: Data?
    do {
        dataOptional = try Data(contentsOf: URL(fileURLWithPath: (path as NSString).expandingTildeInPath), options: [])
    } catch let error as NSError {
        Fail(Error.inputRead, message: String(format:"Error reading -input %@: %@", path, error.localizedDescription))
    }
    let data = dataOptional!
    
    do {
        object = try JSONSerialization.jsonObject(with: data, options: JSONSerialization.ReadingOptions.mutableContainers)
    } catch let error as NSError {
        Fail(Error.inputDeserialize, message: String(format:"Error deserializing %@: %@", path, error.localizedDescription))
    }
    
    guard let key = parameters["key"] as? String else {
        Fail(Error.keyMissing, message: String(format:"-key required if -input specified"))
    }
    
    guard let dictionary = object as? NSMutableDictionary else {
        Fail(Error.inputFormat, message: String(format:"-input %@ was not a dictionary", path))
    }
    dictionary[key] = records
    
} else {
    if let key = parameters["key"] as? String {
        object = [key:records]
    } else {
        object = records as AnyObject
    }
}

// Serialize the object to JSON.
let JSON:String;
do {
    let serialized = try JSONSerialization.data(withJSONObject: object, options: JSONSerialization.WritingOptions.prettyPrinted)
    guard let string = String(data:serialized, encoding:String.Encoding.utf8) else {
        Fail(Error.outputWrite, message: String(format:"Error converting output to string"))
    }
    JSON = string;
} catch let error as NSError {
    Fail(Error.outputSerialize, message: String(format:"Error serializing output: %@", error.localizedDescription))
}


// Write out the result.
let outputPath = (parameters["output"] as? String) ?? (parameters["out"] as? String)
if let path = outputPath {
    // …to a file
    do {
        try JSON.write(toFile: (path as NSString).expandingTildeInPath, atomically:true, encoding: String.Encoding.utf8)
    } catch let error as NSError {
        Fail(Error.outputWrite, message: String(format:"Error writing output %@: %@", path, error.localizedDescription))
    }
} else {
    // …or to stdout
    print(JSON)
}
