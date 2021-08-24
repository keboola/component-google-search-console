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
   


### Sample configuration parameters

```json
{
  "parameters": {
    "date_to": "3 days ago",
    "date_from": "5 days ago",
    "date_range": "Custom",
    "endpoint": "Search analytics",
    "search_analytics_dimensions": "page, query, date",
    "domain": "domain.cz",
    "out_table_name": "search_analytics",
    "filter_groups": [
      [
        {
          "dimension": "query",
          "operator": "contains",
          "expression": "x"
        }
      ],
      [
        {
          "dimension": "query",
          "operator": "contains",
          "expression": "y"
        }
      ]
    ]
  },
  "authorization": {
    "oauth_api": {
      "id": "OAUTH_API_ID",
      "credentials": {
        "id": "main",
        "authorizedFor": "Myself",
        "creator": {
          "id": "1234",
          "description": "me@keboola.com"
        },
        "created": "2016-01-31 00:13:30",
        "#data": "{\"refresh_token\":\"TOKENHERE\"}",
        "oauthVersion": "2.0",
        "appKey": "APIKEY.apps.googleusercontent.com",
        "#appSecret": "SECRET"
      }
    }
  }
}
```

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the docker-compose file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone repo_path my-new-component
cd my-new-component
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)