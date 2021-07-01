from google.oauth2.credentials import Credentials
from retry import retry
from google.auth.transport import requests

from apiclient import discovery
from googleapiclient.errors import HttpError


class ClientError(Exception):
    pass


RETRYABLE_ERROR_CODES = ["concurrentLimitExceeded", "dailyLimitExceeded", "dailyLimitExceededUnreg", "limitExceeded",
                         "quotaExceeded", "rateLimitExceeded", "rateLimitExceededUnreg", "userRateLimitExceeded",
                         "userRateLimitExceededUnreg", "variableTermExpiredDailyExceeded", "variableTermLimitExceeded",
                         "dailyLimitExceeded402", "quotaExceeded402", "servingLimitExceeded"]


class RetryableException(Exception):
    pass


class GoogleSearchConsoleClient:
    def __init__(self, client_id, client_secret, refresh_token, token_uri):
        credentials = Credentials(None, client_id=client_id,
                                  client_secret=client_secret,
                                  refresh_token=refresh_token,
                                  token_uri=token_uri)
        request = requests.Request()
        credentials.refresh(request)
        self.service = discovery.build(
            serviceName='webmasters',
            version='v3',
            credentials=credentials,
            cache_discovery=False
        )

    def get_verified_sites(self):
        site_list = self.service.sites().list().execute()

        # Filter for verified websites
        verified_sites_urls = [s['siteUrl'] for s in site_list['siteEntry']
                               if s['permissionLevel'] != 'siteUnverifiedUser'
                               and s['siteUrl'][:4] == 'http']
        return verified_sites_urls

    def get_search_analytics_data(self, start_date, end_date, url, dimensions):
        request = {
            'startDate': str(start_date),
            'endDate': str(end_date),
            'dimensions': dimensions
        }
        return self.get_all_pages(request, url)

    def get_all_pages(self, request, url):
        row_limit = 25000
        start_row = 0
        response_data = []
        while True:
            request["rowLimit"] = row_limit
            request["startRow"] = start_row
            response = self.execute_search_analytics_request(self.service, url, request)
            if "rows" in response:
                data = response["rows"]
                response_data.extend(data)
                if len(data) != row_limit:
                    break
                start_row = start_row + row_limit
            else:
                break
        return response_data

    def execute_search_analytics_request(self, service, property_uri, request):
        return self._execute_search_analytics_request(service, property_uri, request)

    @staticmethod
    @retry(RetryableException, tries=3, delay=60, jitter=600)
    def _execute_search_analytics_request(service, property_uri, request):
        try:
            return service.searchanalytics().query(siteUrl=property_uri, body=request).execute()
        except HttpError as http_error:
            if http_error.error_details[0]["reason"] in RETRYABLE_ERROR_CODES:
                raise RetryableException(http_error.error_details[0]["reason"]) from http_error
            else:
                raise ClientError(http_error)

    def get_sitemaps_data(self, url):
        sitemaps = self._get_sitemaps_data(url)
        return sitemaps

    @retry(RetryableException, tries=3, delay=60, jitter=600)
    def _get_sitemaps_data(self, url):
        try:
            sitemaps = self.service.sitemaps().list(siteUrl=url).execute()["sitemap"]
            return sitemaps
        except HttpError as http_error:
            if http_error.error_details[0]["reason"] in RETRYABLE_ERROR_CODES:
                raise RetryableException(http_error.error_details[0]["reason"]) from http_error
            else:
                raise ClientError(http_error)
