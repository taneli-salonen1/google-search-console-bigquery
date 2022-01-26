# Export Search Console data to BigQuery

This is a Google Cloud Function that is used to periodically export Search Console data to BigQuery.

Functionality:
1. The function is triggered by a Pub/Sub event
2. A BigQuery dataset and table are created if they didn't exist yet.
3. The BigQuery table is queried to see how fresh the data there is.
4. Search Console API is requested for new data that doesn't already exist in BigQuery.
5. New data is sent to BigQuery.

For a tutorial how to set up the export check this blog post:
https://tanelytics.com/export-google-search-console-data-to-bigquery-using-cloud-functions/