Use -h for help.

Config File Structure
See configs/search.json as an example
    search_api: The entire URL used to access an ESGF search API. Required.
    data_node_priority: A list of strings that must match exactly data node names that should be preferred over other data nodes, from high priority to low priority. The entire result set will be returned using data nodes not present in this list, if needed. Use -data.nodes to find valid values for a given search. Wildcard and regular expressions, as discussed below, are not   supported for the values in this list.  Default [], no priority.
    fields:  Key/value pairs that used to select files to download. Default {}, no fields.

Logic

Logically, the key/value pairs within a given fields object are ANDed together. Users may combine arbitrary AND or OR conditions with appropriate parentheses within a single field.
For example:
    ”field_name”: “value1 OR (value2 AND value3)”
Note that each valueN above may include wildcards or be regular expressions. See Regex vs Wildcard below.

Special Field Considerations

    retracted: This is hard coded to ”false”. User specified values will be ignored.
    latest:  This is hard coded to ”true”. User specified values will be ignored. Note this may conflict with any version specifications, including any ID's that may contain versions.
    replica: This is changed at various points in sproket to ensure users receive one, and only one, copy of each file in a result set. User specified values will be ignored.
    data_node: This is hard coded to ”*”. User specified values will be ignored. See the data_node_priority parameter above for data node control.

Negation

A field key/value match can be negated by prefixing the field key with a dash like so, ”-project”: “CMIP6”. Doing this to any fields in the Special Field Considerations section will result in undefined behavior.

Regex vs Wildcard

It is possible to specify regular expressions for a value in the fields key/value pairs. This requires wrapping the expression like so /regex/ as well as ensuring relevant characters are properly escaped.
”variable_id”: ”/ps|mr(.*)/”

Wildcards are a little different than regular expressions. The wildcards available are ? and *, which match 0 to 1 and 0 to many of any characters, respectively. These do not require the wrapping in backslashes, for example, combining with negation to avoid a whole class of experiments:
”-experiment_id”: “*a4SST*”

Files Collection

Note that this search will be applied to the files collection, but don’t worry, it contains the same attributes as the datasets collection. To access a specific data set the user will need to specify a dataset_id rather than simply id.


Sample Commands

# Download according to search.json
./sproket -config search.json

# "--" == "-", so --config works just as well as -config

# Helpful things to do before actually downloading
#  Check help
sproket -h
#  Check version
sproket -version
#  Count files
sproket -config search.json -count
#  Dry-run with verbose output
sproket -config search.json -no.download -verbose

# Helpful commands for refining search.json
#  Check valid field keys that can be used in the "fields" option
sproket -config search.json -field.keys
#  Check data nodes that can serve the result set
sproket -config search.json -data.nodes


# If there is no time to waste
sproket -config search.json -no.verify -p 32