# SQLPackRatJSON

SQLPackRatJSON is a simple command line tool to output the results of a query as JSON.

    SQLPackRatJSON -database <path> [-bindings <path>] (-query "SQL" | -querypath <path>) [-key <keyname> [-input <path>]]

Runs a query and outputs the result as JSON.

Output takes one of three formats: Plain JSON array, JSON dictionary with only one element, JSON dictionary from existing JSON file.

    SQLPackRatJSON -database Database.db -query "SELECT * FROM Table;"

Records are output as a simple JSON array.

    SQLPackRatJSON -database Database.db -query "SELECT * FROM Table;" -key "Foo"

Records are output as a JSON dictionary, with the array naamed *key*.

    SQLPackRatJSON -database Database.db -query "SELECT * FROM Table;" -key "Foo" -input "existing.json"

Records are output as a JSON dictionary, with the array naamed *key*. Other keys are copied from the existing file.
