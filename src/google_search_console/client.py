from google.oauth2.credentials import Credentials
from retry import retry
from google.auth.transport import requests
from apiclient import discovery
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError
from .exception import ClientError, RetryableException
from typing import Dict, List
from datetime import date

API_ROW_LIMIT = 25000
RETRYABLE_ERROR_CODES = ["concurrentLimitExceeded", "dailyLimitExceeded", "dailyLimitExceededUnreg", "limitExceeded",
                         "quotaExceeded", "rateLimitExceeded", "rateLimitExceededUnreg", "userRateLimitExceeded",
                         "userRateLimitExceededUnreg", "variableTermExpiredDailyExceeded", "variableTermLimitExceeded",
                         "dailyLimitExceeded402", "quotaExceeded402", "servingLimitExceeded"]


class GoogleSearchConsoleClient:
    def __init__(self, client_id: str, client_secret: str, refresh_token: str, token_uri: str) -> None:
        credentials = Credentials(None, client_id=client_id,
                                  client_secret=client_secret,
                                  refresh_token=refresh_token,
                                  token_uri=token_uri)
        request = requests.Request()
        try:
            credentials.refresh(request)
        except RefreshError:
            raise ClientError("Invalid credentials, please re-authenticate the application")
        self.service = discovery.build(serviceName='webmasters',
                                       version='v3',
                                       credentials=credentials,
                                       cache_discovery=False)

    def get_verified_sites(self):
        site_list = self.service.sites().list().execute()

        verified_sites_urls = [s['siteUrl'] for s in site_list['siteEntry']
                               if s['permissionLevel'] != 'siteUnverifiedUser'
                               and s['siteUrl'][:4] == 'http']
        return verified_sites_urls

    def get_search_analytics_data(self, start_date: date, end_date: date, url: str, dimensions: List[str],
                                  filter_groups: List[Dict] = None) -> List[Dict]:
        request: Dict = {
            'startDate': str(start_date),
            'endDate': str(end_date),
            'dimensions': dimensions,
            "dimensionFilterGroups": []
        }
        for filters in filter_groups:
            request["dimensionFilterGroups"].append({"groupType": "and", "filters": filters})
        return self.get_all_pages(request, url)

    def get_all_pages(self, request: Dict, url: str) -> List[Dict]:
        row_limit = API_ROW_LIMIT
        start_row = 0
        response_data = []
        last_page = False
        while not last_page:
            request["rowLimit"] = row_limit
            request["startRow"] = start_row
            response = self.execute_search_analytics_request(self.service, url, request)
            if "rows" in response:
                data = response["rows"]
                response_data.extend(data)
                if len(data) != row_limit:
                    last_page = True
                start_row = start_row + row_limit
            else:
                last_page = True
        return response_data

    def execute_search_analytics_request(self, service, property_uri: str, request: Dict) -> Dict:
        return self._execute_search_analytics_request(service, property_uri, request)

    @retry(RetryableException, tries=3, delay=60, jitter=600)
    def _execute_search_analytics_request(self, service, property_uri: str, request: Dict) -> Dict:
        try:
            return service.searchanalytics().query(siteUrl=property_uri, body=request).execute()
        except HttpError as http_error:
            self._process_exception(http_error)

    def get_sitemaps_data(self, url: str) -> List[Dict]:
        sitemaps = self._get_sitemaps_data(url)
        return sitemaps

    @retry(RetryableException, tries=3, delay=60, jitter=600)
    def _get_sitemaps_data(self, url: str) -> List[Dict]:
        try:
            sitemaps = self.service.sitemaps().list(siteUrl=url).execute()["sitemap"]
            return sitemaps
        except HttpError as http_error:
            self._process_exception(http_error)

    @staticmethod
    def _process_exception(http_error):
        if http_error.error_details[0]["reason"] in RETRYABLE_ERROR_CODES:
            raise RetryableException(http_error.error_details[0]["reason"]) from http_error
        else:
            raise ClientError(http_error)
