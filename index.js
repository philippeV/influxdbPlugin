'use strict';

const async = require('async');
const util = require('util');
const Influx = require('influx');
const app = require('./webserver')();

// Set local caching
var cache = {cached_at: null, cache: null, interval: 15 * 60 * 1000};


//-----------------------------------------------------------------------------------------------------------------
//---------------------------------------------Endpoints-----------------------------------------------------------
//-----------------------------------------------------------------------------------------------------------------

// 1. List datasets
app.get('/datasets', function (req, res) {
    //if (req.headers['x-secret'] !== process.env.CUMULIO_SECRET)
    //return res.status(403).end('Given plugin secret does not match Cumul.io plugin secret.');
    const getSchemaAsync = util.promisify(getSchema);

    getSchemaAsync(req)
        .then(schema => {
            return res.status(200).json(schema)
        })
        .catch(error => {
            return res.status(500).end('InfluxDB internal server error.')
        });
});

// 2. Retrieve data slices
app.post('/query', function (req, res) {
    //if (req.headers['x-secret'] !== process.env.CUMULIO_SECRET)
    //return res.status(403).end('Given plugin secret does not match Cumul.io plugin secret.');

    console.log('test1');
    var isFieldInQuery = false;
    var measurement = req.body.id.split("::");
    measurement = measurement[1];
    var filters = req.body.filters;
    var isColumnLevelPresent = false;
    var columns = req.body.columns;
    var pushdown = false;
    var queryString = 'select';
    var completeQuery = '';
    var selectionArray = [];
    var groupByArray = [];
    var aggregationArray = [];
    var groupByString = '';
    const queryLimiter = 'LIMIT 100000';
    var filterQueryString = '';
    var filterInKeyValues = {};
    var isFirstColumn = true;
    const getColumnsAsync = util.promisify(getColumns);
    var databaseName;
    var host;
    let tempAmountOfFields =0;
    let amountOfFields =0;
    let indexOf
    if (req.headers['x-host'].lastIndexOf("/") !== req.headers['x-host'].length - 1) {
        databaseName = req.headers['x-host'].substring(req.headers['x-host'].lastIndexOf("/") + 1);
        host = req.headers['x-host'].substring(0, req.headers['x-host'].lastIndexOf("/"));
    }
    else {
        return res.status(500).end("error: hostname and database name are incorrect")
    }

    var fieldsAndTagsArray;
    const influxClient = new Influx.InfluxDB({
        host: host,
        database: databaseName,
    });

    getColumnsAsync(measurement, influxClient)
        .then(fieldsAndTags => {
            fieldsAndTagsArray = fieldsAndTags;
            if (req.body.options != null) {
                pushdown = req.body.options.pushdown;
            }
            else {
                pushdown = true;
            }

            // First iteration over columns to set selection array and group by arrray.
            // This is to prevent wrong selections and to parse the endresult
            columns.forEach(function (column, index) {
                fieldsAndTags.forEach(fieldOrTag => {
                    if (fieldOrTag.name.en.includes("field:") && index === 0){
                        tempAmountOfFields++;
                    }
                        if (fieldOrTag.id.localeCompare(column.column_id, 'en', {sensitivity: 'base'}) === 0) {
                            column.column_id = fieldOrTag.id;
                            var influxColumnType;
                            column['dataType'] = fieldOrTag.type;
                            if (fieldOrTag.name.en.includes("tag:")) {
                                influxColumnType = 'tag';
                                column['columnType'] = influxColumnType;
                            }
                            else if (fieldOrTag.name.en.includes("field:")) {
                                influxColumnType = 'field';
                                column['columnType'] = influxColumnType;
                            }
                            else {
                                influxColumnType = 'time';
                                column['columnType'] = influxColumnType;
                            }
                        }
                    }
                );
                if (!pushdown) {

                }
                else if (column.level != null) {
                    isColumnLevelPresent = true;
                    selectionArray.push([column.column_id, column.level]);
                }
                else if (column.aggregation != null) {
                    selectionArray.push([column.column_id, column.aggregation]);
                    aggregationArray.push([column.column_id, column.aggregation]);
                }
                else {
                    if (column.columnType === "field" && column.dataType !== "hierarchy") {
                        column['aggregation'] = 'median';
                        selectionArray.push([column.column_id, column.aggregation]);
                        aggregationArray.push([column.column_id, column.aggregation]);
                    }
                    else {
                        if (column.column_id !== 'time') {
                            selectionArray.push([column.column_id, "group by"]);
                            groupByArray.push(column.column_id);
                        }
                        else {
                            selectionArray.push([column.column_id, "group by"]);
                        }
                    }
                }
            });
            var isTimeLevelSet = false;
            //second iteration over columns to construct selection string and group by string
            columns.forEach(column => {
                if (!pushdown) {
                    if (!isFirstColumn) {
                        queryString += ', ';
                    }
                    queryString += ' \"' + column.column_id + '\"';
                    selectionArray.push(column.column_id);
                    isFirstColumn = false;
                }
                else if (column.level != null) {
                    if (!isFirstColumn) {
                        queryString += ',';
                    }
                    if(column.column_id === 'time') {
                        queryString += ' \"' + column.column_id + '\"';
                        groupByString = columnLevelsToInfluxQuery(column.level, groupByString);
                        isTimeLevelSet = true;
                        isFirstColumn = false;
                    }
                }
                else if (column.aggregation != null) {
                    if (column.column_id === 'time' && !isColumnLevelPresent && !isTimeLevelSet) {
                        groupByString = columnLevelsToInfluxQuery('second', groupByString);
                        isTimeLevelSet = true;
                    }
                    if (column.column_id !== 'time') {
                        isFieldInQuery = true;
                    }
                    if (!isFirstColumn) {
                        queryString += ',';
                    }
                    if (column.column_id !== "*") {
                        queryString += ' ' + column.aggregation + '(\"' + column.column_id + '\")';
                    }
                    else {
                        amountOfFields = tempAmountOfFields;
                        queryString += ' ' + column.aggregation + '(' + column.column_id + ')';
                    }
                    isFirstColumn = false;

                }
                else {
                    if (groupByArray.length > 0 && column.column_id !== 'time') {
                        if (groupByString === '') {
                            groupByString = 'GROUP BY \"' + column.column_id + '\"';
                        }
                        else {
                            groupByString += ', \"' + column.column_id + '\"';
                        }
                    }
                    else {
                        if (column.column_id !== 'time') {
                            if (groupByString === '') {
                                groupByString = 'GROUP BY \"' + column.column_id + '\"';
                            }
                            else {
                                groupByString += ', \"' + column.column_id + '\"';
                            }
                        }
                        if (!isFirstColumn) {
                            queryString += ',';
                        }
                        queryString += ' \"' + column.column_id + '\"';
                        isFirstColumn = false;
                    }

                }
            });

            //completion of the query string
            if (!isFieldInQuery) {
                if (!isFirstColumn) {
                    queryString += ',';
                }
                if (aggregationArray.length > 0 || groupByArray.length > 0) {
                    queryString += ' count(\"' + fieldsAndTags[1].id + '\")';
                }
                else {
                    queryString += ' \"' + fieldsAndTags[1].id + '\"';
                }
            }
            queryString += ' from \"' + measurement + '\"';

            // filter string is constructed by parsing filters array
            if (filters && filters.length) {
                filterQueryString = applyFilters(filters, fieldsAndTags, filterInKeyValues);
            }

            //entire query is assembled
            completeQuery += queryString;
            if (filterQueryString !== '') {
                completeQuery += ' ' + filterQueryString;
            }
            if (groupByString !== '') {
                completeQuery += ' ' + groupByString + ' fill(none)';
            }
            completeQuery += ' ' + queryLimiter;
            console.log(completeQuery);

            //execution of query in influxdb
            return influxClient.queryRaw(completeQuery)
        })
        .then(results => {
            if (pushdown) { //if it was a push down request the data needs to be parsed prior to sending it back to cumul.io
                let newResults = parseResultsToCumulioFormat(results, isFieldInQuery, groupByArray, selectionArray, aggregationArray, filterInKeyValues, amountOfFields);
                return res.status(200).json(newResults);
            }
            else {
                let newResults = parseNonePushDownToCumulioFormat(results, selectionArray);
                return res.status(200).json(newResults);
            }
        })
        .catch(error => {
            return res.status(500).end(error);
        });
});

//3. Authorize not yet implemented currently works as bypass for database name
app.post('/authorize', function (req, res) {
    return res.status(200).json('');
});
//-----------------------------------------------------------------------------------------------------------------
//-----------------------------------------helper functions--------------------------------------------------------
//-----------------------------------------------------------------------------------------------------------------

// retrieving schema of the database
const getSchema = (req, callback) => {
    const getColumnsAsync = util.promisify(getColumns);
    var schema = [];
    var databaseName;
    var host;
    if (req.headers['x-host'].lastIndexOf("/") !== req.headers['x-host'].length - 1) {
        databaseName = req.headers['x-host'].substring(req.headers['x-host'].lastIndexOf("/") + 1);
        host = req.headers['x-host'].substring(0, req.headers['x-host'].lastIndexOf("/"));
    }
    else {

        return callback(new Error("error: hostname and database name are incorrect"));
    }
    var influxClient = new Influx.InfluxDB({
        host: host,
        database: databaseName,
    });
    var measurementsArray = [];
    console.log(databaseName);
    console.log(host);

    //All measurements are gathered and then iterated over to get the complete schema
    influxClient.getMeasurements()
        .then(measurements => {
            let promises = [];
            measurementsArray = measurements;
            measurements.forEach(measurement => {
                promises.push(getColumnsAsync(measurement, influxClient));
            });
            return Promise.all(promises)
        })
        .then(values => {
            let measurementsIndex = 0;
            values.forEach(columns => {
                schema.push({
                    id: databaseName + "::" + measurementsArray[measurementsIndex],
                    name: {en: measurementsArray[measurementsIndex]},
                    description: {en: `All data gathered from measurement: ${measurementsArray[measurementsIndex]} (${databaseName})`},
                    columns: columns
                });
                measurementsIndex++;
            })
        })
        .then(() => {
            //console.log('to cache');
            cache.cached_at = (new Date()).getTime();
            cache.cache = schema;
            //console.log('to cache');
            return callback(null, schema);
        })
        .catch(error => {
            console.log(error);
            return callback(error);
        });
};

// collecting column name and type for given measurement
const getColumns = (measurement, influxClient, callback) => {
    var columns = [];
    var getFieldsQuery = 'SHOW FIELD KEYS FROM ';
    var getTagsQuery = 'SHOW TAG KEYS FROM ';
    var fieldResultsArray;
    influxClient.query(getFieldsQuery + '\"' + measurement + '\"')
        .then(fieldResults => {
            fieldResultsArray = fieldResults;
            return influxClient.query(getTagsQuery + '\"' + measurement + '\"')
        })
        .then(tagResults => {
            columns.push({
                id: 'time',
                name: {en: 'timestamp'},
                type: toCumulioType('time')
            });

            function parseFields(field) {
                //console.log('push field');
                columns.push({
                    id: field.fieldKey,
                    name: {en: "field:" + field.fieldKey},
                    type: toCumulioType(field.fieldType)
                });
            }

            function parseTags(tag) {
                //console.log('push tag');
                columns.push({
                    id: tag.tagKey,
                    name: {en: "tag:" + tag.tagKey},
                    type: toCumulioType('string')
                });
            }

            fieldResultsArray.forEach(parseFields);
            tagResults.forEach(parseTags);
            //console.log('end of getcolumns')
            return callback(null, columns);
        })
        .catch(error => {
            response.status(500).json({error});
            return callback(error);
        });
};

// transforming influxdb data type to cumulio data type
function toCumulioType(influxDBType) {
    switch (influxDBType) {
        case 'integer':
            return 'numeric';
        case 'float' :
            return 'numeric';
        case 'time':
            return 'datetime';
        default:
            return 'hierarchy';

    }
}

function columnLevelsToInfluxQuery(level, groupByString) {
    if (groupByString === '') {
        groupByString = 'GROUP BY';
    }
    else {
        groupByString += ',';
    }

    switch (level) {
        case 'year':
            groupByString += ' time(365d)';
            break;
        case 'quarter':
            groupByString += ' time(91d)';
            break;
        case 'month':
            groupByString += ' time(30d)';
            break;
        case 'week':
            let date = new Date();
            groupByString += ' time(1w,' + date.getDay() + 'd)';
            break;
        case 'day':
            groupByString += ' time(1d)';
            break;
        case 'hour':
            groupByString += ' time(1h)';
            break;
        case 'minute':
            groupByString += ' time(1m)';
            break;
        case 'second':
            groupByString += ' time(1s)';
            break;
        case 'millisecond':
            groupByString += ' time(1ms)';
            break;
        case 'microsecond':
            groupByString += ' time(1u) ';
            break;
        case 'nanosecond':
            groupByString += ' time(1n)';
            break;
    }
    return groupByString;
}

function parseResultsToCumulioFormat(resultsArray, isFieldInQuery, groupByArray, selectionArray, aggregationArray, filterInKeyValues, amountOfFields) {
    var newResults = [];
    resultsArray.results[0].series.forEach(serie => {
        var serieTagValues = [];
        groupByArray.forEach(groupby => {
            serieTagValues.push(serie.tags[groupby]);
        });

        serie.values.forEach(value => {
            var tempValue = [];
            selectionArray.forEach(function(selection, index) {
                let amountOfAggregationsOnEveryColumnLower = 0;
                    for(var i=index;i<selectionArray.length;i++) {
                        if (selectionArray[i][0] === "*" && selectionArray[i][1] === "count") {
                            amountOfAggregationsOnEveryColumnLower++;
                        }
                    }
                if (groupByArray.includes(selection[0])) {
                    tempValue.push(serieTagValues[groupByArray.indexOf(selection[0])]);
                }
                else if (indexOf(aggregationArray, selection) !== -1 && selection[0] !== "time") {
                    var arrayIndex = aggregationArray.length;
                    arrayIndex = value.length - (arrayIndex - indexOf(aggregationArray, selection));
                    if (!isFieldInQuery) {
                        arrayIndex--;
                    }
                    if(amountOfFields > 1)
                    {
                        arrayIndex -= (amountOfFields*amountOfAggregationsOnEveryColumnLower -1);
                    }
                    if (value[arrayIndex] == null) {

                        if (filterInKeyValues.hasOwnProperty(selection[0])) {
                            tempValue.push(filterInKeyValues[selection[0]]);
                        }
                        else {

                            tempValue.push('...');
                        }
                    }
                    else
                        tempValue.push(value[arrayIndex]);
                }
                else {
                    if (value[serie.columns.indexOf(selection[0])] == null) {
                        tempValue.push('...');
                    }
                    else
                        tempValue.push(value[serie.columns.indexOf(selection[0])]);


                }

            });
            newResults.push(tempValue);
        });
    });
    return newResults;
}

function parseNonePushDownToCumulioFormat(resultsArray, selectedColumns) {
    var newResultsArray = [];
    resultsArray.results[0].series[0].values.forEach(value => {
        let tempValue = [];
        selectedColumns.forEach(selectedColumn => {
            if(resultsArray.results[0].series[0].columns.includes(selectedColumn)) {
                tempValue.push(value[resultsArray.results[0].series[0].columns.indexOf(selectedColumn)])
            }
        })
        newResultsArray.push(tempValue);
    });
    return newResultsArray;

}

function applyFilters(filterArray, fieldsAndTags, filterInKeyValues) {
    if (filterArray.length === 0) {
        return ''
    }
    var completefilterString = 'WHERE';
    var tempFilterString = '';

    filterArray.forEach(function (filter) {
        let value;
        let isTagOrTime = false;
        let column;
        let values;

        fieldsAndTags.forEach(fieldOrTag => {
            if (fieldOrTag.id.localeCompare(filter.column_id, 'en', {sensitivity: 'base'}) === 0) {
                column = fieldOrTag;
                filter.column_id = fieldOrTag.id;
                if (fieldOrTag.id === "time" || fieldOrTag.name.en.includes("tag:")) {
                    isTagOrTime = true;
                }
            }
        });

        if (filter.expression !== 'in' && filter.expression !== 'not in' && filter.expression !== 'is not null' && filter.expression !== 'is null') {
            value = filter.value[0];
        }
        else {
            values = filter.value;
        }

        if (filter.column_id === 'time' && (filter.expression === 'is not null' || filter.expression === 'is null'))        //converter for dates
        {
            return
        }

        if (filter.column_id === 'time' && filter.expression !== 'is not null' && filter.expression !== 'is null')        //converter for dates
        {
            var date = new Date(filter.value[0]);
            value = date.getTime();
        }

        let i;
        switch (filter.expression) {
            case 'in':

                filterInKeyValues[filter.column_id] = values[0];
                tempFilterString += '(';
                for (i = 0; i < values.length; i++) {
                    if (i !== 0) {
                        tempFilterString += ' OR';
                    }
                    if (!isTagOrTime) {
                        if (column.type === "hierarchy") {
                            tempFilterString += ' \"' + filter.column_id + '\" = \'' + values[i] + '\'';
                        }
                        else {
                            tempFilterString += ' \"' + filter.column_id + '\" = ' + values[i];
                        }
                    }
                    else {
                        tempFilterString += ' \"' + filter.column_id + '\" = \'' + values[i] + '\'';
                    }
                }
                tempFilterString += ')';
                completefilterString += tempFilterString;
                tempFilterString = ' AND';
                break;
            case 'not in':
                filterInKeyValues[filter.column_id] = values[0];
                tempFilterString += '(';
                for (i = 0; i < values.length; i++) {
                    if (i !== 0) {
                        tempFilterString += ' OR';
                    }
                    if (!isTagOrTime) {
                        if (column.type === "hierarchy") {
                            tempFilterString += ' \"' + filter.column_id + '\" <> \'' + values[i] + '\'';
                        }
                        else {
                            tempFilterString += ' \"' + filter.column_id + '\" <> ' + values[i];
                        }
                    }
                    else {
                        tempFilterString += ' \"' + filter.column_id + '\" <> \'' + values[i] + '\'';
                    }
                }
                tempFilterString += ')';
                completefilterString += tempFilterString;
                tempFilterString = ' AND';
                break;
            case '<':
                tempFilterString += ' \"' + filter.column_id + '\" < ' + value;
                if (filter.column_id === 'time') {
                    tempFilterString += 'ms';
                }
                completefilterString += tempFilterString;
                tempFilterString = ' AND';
                break;
            case '<=':
                tempFilterString += ' \"' + filter.column_id + '\" <= ' + value;
                if (filter.column_id === 'time') {
                    tempFilterString += 'ms';
                }
                completefilterString += tempFilterString;
                tempFilterString = ' AND';
                break;
            case '>=':
                tempFilterString += ' \"' + filter.column_id + '\" >= ' + value;
                if (filter.column_id === 'time') {
                    tempFilterString += 'ms';
                }
                completefilterString += tempFilterString;
                tempFilterString = ' AND';
                break;
            case '>':
                tempFilterString += ' \"' + filter.column_id + '\" > ' + value;
                if (filter.column_id === 'time') {
                    tempFilterString += 'ms';
                }
                completefilterString += tempFilterString;
                tempFilterString = ' AND';
                break;
            case '=':
                if (!isTagOrTime) {
                    if (column.type === "string") {
                        tempFilterString += ' \"' + filter.column_id + '\" = \'' + value + '\'';
                    }
                    else {
                        tempFilterString += ' \"' + filter.column_id + '\" = ' + value;
                    }
                }
                else {
                    tempFilterString += ' \"' + filter.column_id + '\" = ' + value;
                    completefilterString += tempFilterString;
                }
                completefilterString += tempFilterString;
                tempFilterString = ' AND';
                break;
            case 'is null':
                if (!isTagOrTime) {
                    break;
                }
                else {
                    tempFilterString += ' \"' + filter.column_id + '\" = \'\'';
                    completefilterString += tempFilterString;
                    tempFilterString = ' AND';
                    break;
                }
            case 'is not null':
                if (!isTagOrTime) {
                    break;
                }
                else {
                    tempFilterString += ' \"' + filter.column_id + '\" <> \'\'';
                    completefilterString += tempFilterString;
                    tempFilterString = ' AND';
                    break;
                }
        }


    });
    if (tempFilterString === '') {
        return '';
    }
    return completefilterString;


}

function indexOf(arrayToBeChecked, array) {
    for (let index = 0; index < arrayToBeChecked.length; index++) {
        if (arrayToBeChecked[index][0] === array[0] && arrayToBeChecked[index][1] === array[1]) {
            return index;
        }
    }
    return -1;
}