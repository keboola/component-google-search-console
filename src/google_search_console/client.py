from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from retry import retry
from google.auth.transport import requests
from apiclient import discovery
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError
from .exception import ClientError, RetryableException, ClientAuthError
from typing import Dict, List, Generator
from datetime import date
import socket

API_ROW_LIMIT = 25000
RETRYABLE_ERROR_CODES = ["concurrentLimitExceeded", "dailyLimitExceeded", "dailyLimitExceededUnreg", "limitExceeded",
                         "quotaExceeded", "rateLimitExceeded", "rateLimitExceededUnreg", "userRateLimitExceeded",
                         "userRateLimitExceededUnreg", "variableTermExpiredDailyExceeded", "variableTermLimitExceeded",
                         "dailyLimitExceeded402", "quotaExceeded402", "servingLimitExceeded"]


class GoogleSearchConsoleClient:
    def __init__(self, client_id: str, client_secret: str, refresh_token: str,
                 service_account_info: dict = None, token_uri: str = "https://oauth2.googleapis.com/token") -> None:
        if service_account_info:
            credentials = ServiceAccountCredentials.from_service_account_info(service_account_info)
        else:
            credentials = Credentials(None, client_id=client_id,
                                      client_secret=client_secret,
                                      refresh_token=refresh_token,
                                      token_uri=token_uri)
            request = requests.Request()
            try:
                credentials.refresh(request)
            except RefreshError:
                raise ClientError("Invalid credentials, please re-authenticate the application")
        self.service = discovery.build('searchconsole', 'v1', credentials=credentials,
                                       cache_discovery=False)

    def get_verified_sites(self):
        site_list = self.service.sites().list().execute()

        verified_sites_urls = [s['siteUrl'] for s in site_list['siteEntry']
                               if s['permissionLevel'] != 'siteUnverifiedUser'
                               and s['siteUrl'][:4] == 'http']
        return verified_sites_urls

    def get_search_analytics_data(self, start_date: date, end_date: date, url: str, dimensions: List[str],
                                  search_type: str = None, filter_groups: List[Dict] = None) -> Generator:
        request: Dict = {
            'startDate': str(start_date),
            'endDate': str(end_date),
            'dimensions': dimensions,
            "dimensionFilterGroups": []
        }
        if search_type:
            request["type"] = search_type
        for filters in filter_groups:
            request["dimensionFilterGroups"].append({"groupType": "and", "filters": filters})
        return self.get_result_pages(request, url)

    def get_result_pages(self, request: Dict, url: str) -> Generator:
        row_limit = API_ROW_LIMIT
        start_row = 0
        last_page = False
        while not last_page:
            request["rowLimit"] = row_limit
            request["startRow"] = start_row
            response = self.execute_search_analytics_request(self.service, url, request)
            if "rows" in response:
                data = response["rows"]
                yield data
                if len(data) != row_limit:
                    last_page = True
                start_row = start_row + row_limit
            else:
                last_page = True

    def execute_search_analytics_request(self, service, property_uri: str, request: Dict) -> Dict:
        try:
            search_analytics_data = self._execute_search_analytics_request(service, property_uri, request)
            if not search_analytics_data:
                search_analytics_data = self._execute_search_analytics_request(service,
                                                                               "".join(["sc-domain:", property_uri]),
                                                                               request)
            if not search_analytics_data:
                search_analytics_data = self._execute_search_analytics_request(service,
                                                                               "".join(["https://www.", property_uri]),
                                                                               request)
            if not search_analytics_data:
                search_analytics_data = self._execute_search_analytics_request(service,
                                                                               "".join(["http://www.", property_uri]),
                                                                               request)
            if not search_analytics_data:
                search_analytics_data = self._execute_search_analytics_request(service,
                                                                               "".join(["https://", property_uri]),
                                                                               request)
            if not search_analytics_data:
                search_analytics_data = self._execute_search_analytics_request(service,
                                                                               "".join(["http://", property_uri]),
                                                                               request)
            if not search_analytics_data:
                raise ClientAuthError(f"{property_uri} is not a valid Search Console site URL. Make sure you "
                                      f"have sufficient rights if the url is valid")
            return search_analytics_data
        except socket.timeout:
            raise ClientError("Connection timed out, please try a smaller query")

    @retry(RetryableException, tries=3, delay=60, jitter=600)
    def _execute_search_analytics_request(self, service, property_uri: str, request: Dict) -> Dict:
        try:
            return service.searchanalytics().query(siteUrl=property_uri, body=request).execute()
        except HttpError as http_error:
            if http_error.status_code == 403:
                pass
            else:
                self._process_exception(http_error)

    def get_sitemaps_data(self, url: str) -> List[Dict]:
        sitemaps = self._get_sitemaps_data(url)
        if not sitemaps:
            sitemaps = self._get_sitemaps_data("".join(["sc-domain:", url]))
        if not sitemaps:
            sitemaps = self._get_sitemaps_data("".join(["https://www.", url]))
        if not sitemaps:
            sitemaps = self._get_sitemaps_data("".join(["http://www.", url]))
        if not sitemaps:
            sitemaps = self._get_sitemaps_data("".join(["https://", url]))
        if not sitemaps:
            sitemaps = self._get_sitemaps_data("".join(["http://", url]))
        if not sitemaps:
            raise ClientAuthError(f"{url} is not a valid Search Console site URL. Make sure you have sufficient "
                                  f"rights if the url is valid")
        return sitemaps

    @retry(RetryableException, tries=3, delay=60, jitter=600)
    def _get_sitemaps_data(self, url: str) -> List[Dict]:
        try:
            sitemaps = self.service.sitemaps().list(siteUrl=url).execute()["sitemap"]
            return sitemaps
        except HttpError as http_error:
            if http_error.status_code == 403:
                pass
            else:
                self._process_exception(http_error)
        except KeyError:
            raise ClientError(f"Could not fetch sitemaps from the API, the returned data did not contain the sitemaps. "
                              f"Data returned :({self.service.sitemaps().list(siteUrl=url).execute()}) ")

    @staticmethod
    def _process_exception(http_error):
        try:
            print(http_error.error_details)
            if http_error.error_details[0]["reason"] in RETRYABLE_ERROR_CODES:
                raise RetryableException(http_error.error_details[0]["reason"]) from http_error
            else:
                if http_error.reason == 'Request contains an invalid argument.':
                    raise ClientError("Request contains an invalid argument. Make sure all your dimensions and "
                                      "filters are valid along with your search Type filter.")
                raise ClientError(http_error)
        except TypeError:
            raise ClientError(http_error)
