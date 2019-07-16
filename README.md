# Medium stats export
This repo contains 2 functions:
- [Nodejs] Scraping function (using Puppeteer in headless mode) to automate login and export of medium stats to GCS (CSV format)
- [Golang] Function that import the previously exported CSV to Cloud Spanner