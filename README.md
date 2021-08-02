# Google Search Console Extractor
This component allows you to extract statistics and site data of domains that are linked to your Google account.

**Table of contents:**  
  
[TOC]

## Configuration

### Authorization
Authorization is done via instant authorization, link Google account

### Row configuration

 - Domain (domain) - [REQ] Domain name you wish to extract data from eg. keboola.com
 - Endpoint (endpoint) - [REQ] Search analytics or Sitemaps
 - Dimensions (search_analytics_dimensions) - [REQ For Search Analytics] List of search analytics dimensions eg. page, query, date 
 - Date range type (date_range) - [REQ For Search Analytics] Type of date range
    - Last week (sun-sat) used for WEEK dimension
    - Last month (from first day of the previous month to last day of the previous month)
    - Custom - must then specify date from and to (3 days ago to 1 day ago) (1 march 2021 to 23 march 2021)
 - Date from (date_from) - [REQ For Search Analytics] Start date of the report eg. 3 days ago
 - Date to (date_to) - [REQ For Search Analytics] End date of the report eg. 1 day ago
 - Output name (out_table_name) - [REQ] Name of output table in Keboola storage
 - Filters (filters) - [OPT] - list of filter groups:
      - Filters in a single filter group are grouped by "and", therefore if 2 filters are in a filter group, they must both be satisfied to return data
      - Filters in separate filter groups work with "or", therefore at least 1 of the filters must be satisfied to return data

