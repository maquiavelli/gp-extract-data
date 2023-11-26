# Google Play Developer Reporting API ETL Playground

This project aims to deepen the understanding of information capture from the Google Play Developer Reporting Api.

Here are some objectives that will be met during the course of development.

- [x] Create a functional Google Api authentication method;
- [x] Create a replicable standard contract model for obtaining reports;
- [x] Generate JSON files from query responses;
- [x] Transform JSON files into normalized information files;
- [x] Implement a repetition loop to increment reports with a size greater than one thousand objects;
- [x] Use environments parameters to period to get raw data;
- [x] Create a playground MVP based on pandas;
- [x] Build example datasets from the local folder;-
- [x] Create an example dashboard;
- [x] Use environment variables instead of a JSON credentials file;
- [x] Move params to environment file;
- [x] Create folder raw_data for the different apps;
- [x] Create folder datasets for the different apps;
- [x] Create default methods to log;
- [x] Merge data frames;
- [x] Refactor code;
- [X] Discovery about the return of the Error Report query;
- [X] Refactor structure responsible for controlling reports;
- [X] Fix raw_data folders name;
- [X] Create exception when page failed;
- [ ] Implement retry to ssl timeout error or verify if pageToken is resilient;
- [ ] Fix period information for reports;
- [ ] Create methods to transform/load by metrics type and reports type;
- [ ] Verify existant filters. Ex.: If dataset don't have a date value;
- [ ] Create friendly documentation;
- [x] Get massive data;
- [ ] Fix without null dates on merge datasets on Playground;
- [ ] Create workload for ingest data on BigQuery;
- [ ] Create module to increment data;
- [x] Fix datetime from logs;
- [x] Recognize fresh data;
- [x] Disable ErrorCount until you understand its real use
- [x] Separate reports dictionary in json file
- [x] Add userPerceivedAnrRate metric to AnrRateMetricSet
 