const { BigQuery } = require('@google-cloud/bigquery');

// A function for paginating a JS array. Used for paginating the BQ streaming insert request to avoid too large requests.
const paginate = (arr, size) => {
    return arr.reduce((acc, val, i) => {
        let idx = Math.floor(i / size)
        let page = acc[idx] || (acc[idx] = [])
        page.push(val)

        return acc
    }, [])
}

const queryRequest = async (query, project) => {
    // if project is specified use that, else use the automatically detected one of the environment
    //console.log(`Query request with project id: ${project}`)

    const bigquery = project ? new BigQuery({
        projectId: project
    }) : new BigQuery();

    const options = {
        "query": query,
        "useLegacySql": false,
        "kind": "bigquery#queryRequest"
    }

    try {
        const [job] = await bigquery.createQueryJob(options)
        //console.log(`BigQuery job ${job.id} started.`)

        const [rows] = await job.getQueryResults()

        return rows
    } catch (e) {
        if (e && e.response) {
            console.log(`BigQuery query request failed with these errors:\n ${JSON.stringify(e.errors, null, 2)}`)
            //console.log(e)
        } else {
            console.error(e)
        }
    }

    // return null if no results were found, a valid query response can be []
    return null
}

const createDataset = async (project, datasetId, location) => {
    const bigquery = project ? new BigQuery({
        projectId: project
    }) : new BigQuery();

    const options = {
        location: location,
    };

    // Try to create the dataset, if it doesn't already exist
    try {
        const [dataset] = await bigquery.createDataset(datasetId, options);
        console.log(`Dataset ${dataset.id} created.`);

        return dataset.id;
    } catch (e) {
        const errorMsg = e.errors ? e.errors[0]?.message : 'none';
        console.log(`Dataset not created, message: ${errorMsg}`);
        if (errorMsg === 'none') console.log(e);

        if (e && e.code === 409) {
            return datasetId;
        }
    }
}

const createTable = async (project, dataset, tableName) => {
    const bigquery = project ? new BigQuery({
        projectId: project
    }) : new BigQuery();

    const options = {
        schema: {
            fields: exportTableFields
        },
        location: 'EU',
        requirePartitionFilter: false,
        timePartitioning: {
            type: "DAY",
            field: "date"
        },
        description: "Raw export data from Google Search Console"
    };

    try {
        const [table] = await bigquery
            .dataset(dataset)
            .createTable(tableName, options);
     
        // if a new table was just created, the streaming insert will fail
        // https://stackoverflow.com/questions/36415265/after-recreating-bigquery-table-streaming-inserts-are-not-working
        // Instead of waiting, just end the function. Next run should start saving data

        console.log(`Table ${table.id} created.`);
        console.log('Ending script. Streaming insert needs to wait before it\'s functional with the new table. (can take a few minutes)')

        return null;
    } catch (e) {
        const errorMsg = e.errors ? e.errors[0]?.message : 'none';
        console.log(`Table not created, message: ${errorMsg}`);
        if (errorMsg === 'none') console.log(e);

        if (e && e.code === 409) {
            return tableName;
        }
    }

}

const streamingInsert = async (data, project, dataset, table) => {
    const bigquery = new BigQuery({
        projectId: project
    });

    // Map the rows to BQ streaming insert format
    // Paginate to pages of max size 5000
    const rowsPages = paginate(data.map(item => {
        return {
            insertId: item.unique_key, // unique id for each inserted row
            json: item
        }
    }), 5000);

    const options = {
        raw: true
    };

    // Make the insert requests in batches of 5000 rows
    const promises = [];
    rowsPages.forEach(page => {
        promises.push(
            bigquery
                .dataset(dataset)
                .table(table)
                .insert(page, options)
        );
    });

    const allPromises = Promise.all(promises);

    try {
        const responses = await allPromises;
        console.log(`Inserted ${data.length} rows.`);
        return `Inserted ${responses.length} pages of up to 5000 rows.`;
    } catch (e) {
        const errorMsg = e.errors ? e.errors[0]?.message : 'none';
        console.log(`BigQuery insert failed, message: ${errorMsg}`);
        if (errorMsg === 'none') console.log(e);
    }
}

const exportTableFields = [
    {
        "description": "Date of the impression or click",
        "mode": "REQUIRED",
        "name": "date",
        "type": "DATE"
    },
    {
        "description": "Number of clicks on a search result",
        "mode": "REQUIRED",
        "name": "clicks",
        "type": "INTEGER"
    },
    {
        "description": "Number of impressions for the page",
        "mode": "REQUIRED",
        "name": "impressions",
        "type": "INTEGER"
    },
    {
        "description": "Position of the search result (average calculated for the date)",
        "mode": "REQUIRED",
        "name": "position",
        "type": "FLOAT"
    },
    {
        "description": "Page URL",
        "mode": "NULLABLE",
        "name": "page",
        "type": "STRING"
    },
    {
        "description": "Country of the user",
        "mode": "NULLABLE",
        "name": "country",
        "type": "STRING"
    },
    {
        "description": "Device type",
        "mode": "NULLABLE",
        "name": "device",
        "type": "STRING"
    },
    {
        "description": "Search query",
        "mode": "NULLABLE",
        "name": "query",
        "type": "STRING"
    },
    {
        "description": "Unique key for each row inserted to the table. Created by concatenating all dimension values.",
        "mode": "REQUIRED",
        "name": "unique_key",
        "type": "STRING"
    }
];

exports.queryRequest = queryRequest
exports.streamingInsert = streamingInsert
exports.createTable = createTable
exports.createDataset = createDataset