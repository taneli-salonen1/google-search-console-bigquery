const { google } = require("googleapis");
const bq = require('./bigquery');
const md5 = require('md5');
require('dotenv-yaml').config();


// Check BQ for the latest date of already existing data
// Return the start and end of the date range for the new request
const getDateRange = async () => {
    /*
    The query is configured to return max 30 days of data at once to avoid getting too big json files in response.
    The function can be repeated multiple times to get a larger backfill.
    */

    const numDays = process.env.NUM_DAYS_AT_ONCE - 1 || 0;

    const dateRangeQuery = `WITH
    start AS (
    SELECT
      coalesce(MAX(date)+1,
        CAST('${process.env.START_DATE}' AS date)) AS date_range_start
    FROM
    \`${process.env.BIGQUERY_PROJECT}.${process.env.BIGQUERY_DATASET}.${process.env.RESULTS_TABLE}\`)
  SELECT
    CAST(date_range_start AS string) AS date_range_start,
    CAST(date_range_start + ${numDays} AS string) AS date_range_end,
    ARRAY(
    SELECT
      CAST(date AS string)
    FROM
      UNNEST(GENERATE_DATE_ARRAY(date_range_start, date_range_start+${numDays})) AS date) AS dates
  FROM
    start`;

    const result = await bq.queryRequest(dateRangeQuery, process.env.BIGQUERY_PROJECT);

    if (result && result.length > 0) {
        return result[0];
    } else {
        return undefined;
    }
}

const gscRequest = async (startDate, endDate, rowLimit, startRow, dimensions) => {
    /*
    This function makes a call to GSC API and returns the data.
    Multiple calls might be needed because not all data fits under the 25k row limit.
    */

    const auth = await google.auth.getClient({ scopes: ["https://www.googleapis.com/auth/webmasters"], });
    const gsc = google.webmasters({ version: "v3", auth });

    try {
        const res = await gsc.searchanalytics.query({
            siteUrl: process.env.SITE,
            searchType: "web",
            aggregationType: "auto",
            dimensions: dimensions,
            requestBody: {
                startDate: startDate,
                endDate: endDate,
            },
            rowLimit: rowLimit,
            startRow: startRow,
            dataState: 'final'
        });

        //console.log(res.data);
        return res.data;
    } catch (e) {
        const errorMsg = e.errors ? e.errors[0]?.message : 'none';
        console.log(`Search console request failed with message: ${errorMsg}`);
        if (errorMsg === 'none') console.log(e);
    }

};

const getSearchConsoleData = async (date) => {
    /*
    This loops through multiple requests to the GSC API
    If on of the requests fails, the function will error out and no data will be transferred to BigQuery

    This is set up to only return one day's data at once.
    */
    const gscData = [];
    const rowLimit = 25000; // 25k rows is the max that can be returned at once from the API

    let startRow = 0;
    let continueLoop = true;

    const dimensions = [
        'page',
        'country',
        'device',
        'query'
    ];

    do {
        //console.log(`GSC API request from row: ${startRow}`);
        const res = await gscRequest(date, date, rowLimit, startRow, dimensions);
        if (res && res.rows && res.rows.length > 0) {
            // new rows have been found, collect them to the same array
            gscData.push(res.rows);

            if (res.rows.length < rowLimit) {
                continueLoop = false;
            }

            // set the start row for the next iteration
            startRow += res.rows.length;
        } else {
            // no new rows were found, exit
            continueLoop = false;
        }
    } while (continueLoop === true)

    // looping of GSC API requests has completed
    if (gscData.length > 0) {
        console.log(`Retrieved ${gscData.flat().length} rows from Search Console. Date: ${date}`);

        const insertRows = gscData.flat().map(row => {
            const obj = {
                date: date,
                clicks: row.clicks,
                impressions: row.impressions,
                position: row.position
            };

            dimensions.forEach((dim, i) => {
                obj[dim] = row.keys[i]
            });

            // set insert id / unique key here
            obj['unique_key'] = md5(row.keys.join('') + date);

            return obj;
        });

        return insertRows;
    } else {
        return [];
    }
}

const getNewData = async (message, context) => {
    const pubSubMesg = message.data
        ? Buffer.from(message.data, 'base64').toString()
        : null;

    // another option to submit setup data would be to use pub/sub message payload
    console.log(`Pub/Sub message payload: ${pubSubMesg}`);

    const setup = {
        dataset: null,
        table: null
    };

    // check that the dataset and table exist and create them if needed
    setup.dataset = await bq.createDataset(process.env.BIGQUERY_PROJECT, process.env.BIGQUERY_DATASET, process.env.LOCATION);

    // check that the table exists and create if needed
    if (setup.dataset) {
        setup.table = await bq.createTable(process.env.BIGQUERY_PROJECT, setup.dataset, process.env.RESULTS_TABLE);
    }

    // if everything is clear, move forward
    if (setup.dataset && setup.table) {
        // find the daterange for the search console API export
        const dateRange = await getDateRange();
        console.log(`Getting data for ${dateRange.date_range_start} - ${dateRange.date_range_end}`);

        // loop through days, one at a time, fetching the data for the day
        const gscPromises = [];
        dateRange.dates.forEach(date => {
            gscPromises.push(getSearchConsoleData(date, date));
        });

        const gscData = await Promise.all(gscPromises);

        if (gscData.filter(day => day.length > 0).length === 0) {
            console.log('No new data found.')
        } else {
            // start sending the data to BigQuery
            const insertPromises = [];
            gscData.forEach(data => {
                if (data && data.length > 0) {
                    // send the data to BigQuery
                    insertPromises.push(bq.streamingInsert(data, process.env.BIGQUERY_PROJECT, process.env.BIGQUERY_DATASET, process.env.RESULTS_TABLE));
                }
            });

            await Promise.all(insertPromises);
        }
    } else {
        console.log('Setup failed.');
    }
}

exports.getNewData = getNewData;