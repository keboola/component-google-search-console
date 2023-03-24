import logging
import dateparser
import warnings
import csv
from datetime import date
from os import path, mkdir, listdir, rmdir
from datetime import timedelta
from typing import List
from keboola.component.base import ComponentBase, UserException
from google_search_console import GoogleSearchConsoleClient, ClientError, ClientAuthError
from keboola.component.dao import OauthCredentials
from typing import Dict, Tuple, Generator
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
import json


KEY_DOMAIN = 'domain'
KEY_OUT_TABLE_NAME = "out_table_name"
KEY_ENDPOINT = "endpoint"
KEY_SEARCH_ANALYTICS_DIMENSIONS = "search_analytics_dimensions"
KEY_DATE_FROM = "date_from"
KEY_DATE_TO = "date_to"
KEY_DATE_RANGE = "date_range"
KEY_SEARCH_TYPE = "search_type"
KEY_CLIENT_ID = "appKey"
KEY_CLIENT_SECRET = "appSecret"
KEY_REFRESH_TOKEN = "refresh_token"
KEY_FILTER_GROUPS = "filter_groups"
KEY_AUTH_DATA = "data"
KEY_LOADING_OPTIONS = "loading_options"
KEY_LOADING_OPTIONS_INCREMENTAL = "incremental"
KEY_SERVICE_ACCOUNT = '#service_account_info'

SITEMAPS_HEADERS = ["path", "lastSubmitted", "isPending", "isSitemapsIndex", "type", "lastDownloaded", "warnings",
                    "errors"]

SEARCH_TYPES = ["news", "video", "image", "web", "discover", "googleNews"]

REQUIRED_PARAMETERS = [KEY_DOMAIN, KEY_OUT_TABLE_NAME, KEY_ENDPOINT]
REQUIRED_IMAGE_PARS = []

# Ignore dateparser warnings regarding pytz
warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)


class Component(ComponentBase):
    def __init__(self) -> None:
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters
        self.out_table_name = params.get(KEY_OUT_TABLE_NAME)
        self.validate_table_name(self.out_table_name)
        self.out_table_name = "".join([self.out_table_name, ".csv"])
        self.endpoint = params.get(KEY_ENDPOINT)
        self.domain = params.get(KEY_DOMAIN)
        self.filter_groups = params.get(KEY_FILTER_GROUPS, [[]])

        self.service_account_info = params.get(KEY_SERVICE_ACCOUNT, None)

    def run(self) -> None:
        client_id_credentials = self.configuration.oauth_credentials

        if self.service_account_info:
            gsc_client = self.get_gsc_client(service_account_info=self.service_account_info)
        else:
            gsc_client = self.get_gsc_client(client_id_credentials=client_id_credentials)

        logging.getLogger("googleapiclient.http").disabled = True

        if self.endpoint == "Search analytics":
            self.fetch_and_write_search_analytics_data(gsc_client)

        elif self.endpoint == "Sitemaps":
            sitemap_data = self.get_sitemaps_data(gsc_client)
            self.write_results(sitemap_data)

        else:
            raise ValueError("Endpoint selected does not exist")

    def fetch_and_write_search_analytics_data(self, gsc_client: GoogleSearchConsoleClient) -> None:
        params = self.configuration.parameters
        search_analytics_dimensions = self.parse_list_from_string(params.get(KEY_SEARCH_ANALYTICS_DIMENSIONS, ""))
        incremental = params.get(KEY_LOADING_OPTIONS, {}).get(KEY_LOADING_OPTIONS_INCREMENTAL, 0)
        date_downloaded = date.today()
        table = self.create_out_table_definition(self.out_table_name,
                                                 primary_key=search_analytics_dimensions,
                                                 incremental=incremental,
                                                 is_sliced=True)
        self.create_sliced_directory(table.full_path)
        fieldnames = []
        try:
            for i, search_data_slice in enumerate(self.get_search_analytics_data(gsc_client)):
                parsed_slice = self.parse_search_analytics_data(search_data_slice, search_analytics_dimensions)
                slice_path = path.join(table.full_path, str(i))
                fieldnames = list(parsed_slice[0].keys())
                fieldnames.append("date_downloaded")
                fieldnames.append("domain")
                self.write_results_to_out_table(slice_path, fieldnames, parsed_slice, date_downloaded)
            table.columns = fieldnames
            if len(listdir(table.full_path)) != 0:
                self.write_tabledef_manifest(table)
            else:
                logging.warning("No Data Found")
                rmdir(table.full_path)
        except (ClientError, HttpError, ClientAuthError) as cl_error:
            raise UserException(cl_error) from cl_error

    @staticmethod
    def create_sliced_directory(table_path: str) -> None:
        logging.info("Creating sliced file")
        if not path.isdir(table_path):
            mkdir(table_path)

    @staticmethod
    def get_gsc_client(client_id_credentials: OauthCredentials = None, service_account_info: str = "")\
            -> GoogleSearchConsoleClient:
        if service_account_info:
            try:
                service_account_dict = json.loads(service_account_info)
                creds = ServiceAccountCredentials.from_service_account_info(service_account_dict)
                creds = creds.with_scopes(['https://www.googleapis.com/auth/webmasters.readonly'])
                return GoogleSearchConsoleClient(client_id="", client_secret="", refresh_token="",
                                                 service_account_info=creds)
            except ClientError as client_error:
                raise UserException(client_error) from client_error
        elif client_id_credentials:
            client_id = client_id_credentials[KEY_CLIENT_ID]
            client_secret = client_id_credentials[KEY_CLIENT_SECRET]
            refresh_token = client_id_credentials[KEY_AUTH_DATA][KEY_REFRESH_TOKEN]

            try:
                return GoogleSearchConsoleClient(client_id, client_secret, refresh_token)
            except ClientError as client_error:
                raise UserException(client_error) from client_error
        else:
            raise UserException(
                "Component is not authorized, please authorize the app in the authorization configuration ")

    def write_results(self, data: List[Dict]) -> None:
        fieldnames = list(data[0].keys())
        fieldnames.append("date_downloaded")
        fieldnames.append("domain")
        date_downloaded = date.today()
        out_table = self.create_out_table_definition(name=self.out_table_name,
                                                     columns=fieldnames)
        self.write_results_to_out_table(out_table.full_path, fieldnames, data, date_downloaded)
        self.write_tabledef_manifest(out_table)

    def write_results_to_out_table(self, file_path: str, fieldnames: List[str], data: List[Dict],
                                   date_downloaded: date) -> None:
        with open(file_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, fieldnames)
            for result in data:
                result["date_downloaded"] = date_downloaded
                result["domain"] = self.domain
                writer.writerow(result)

    def get_search_analytics_data(self, gsc_client: GoogleSearchConsoleClient) -> Generator:
        params = self.configuration.parameters
        search_analytics_dimensions = self.parse_list_from_string(params.get(KEY_SEARCH_ANALYTICS_DIMENSIONS, ""))
        search_type = params.get(KEY_SEARCH_TYPE)
        if search_type and search_type not in SEARCH_TYPES:
            raise UserException(f"Type must be one of the following {SEARCH_TYPES}, you entered '{search_type}'.")
        if not search_analytics_dimensions:
            raise UserException("Missing Search Analytics dimensions, please fill them in")

        date_from, date_to = self.get_date_range(params.get(KEY_DATE_FROM),
                                                 params.get(KEY_DATE_TO),
                                                 params.get(KEY_DATE_RANGE))

        logging.info(
            f"Fetching data for search analytics for {search_analytics_dimensions} dimensions for domain {self.domain},"
            f"for dates from {date_from} to {date_to}")

        logging.info(f"Filters set as {self.filter_groups}")

        if not self.filter_groups:
            return self._get_search_analytics_data(gsc_client, date_from, date_to, search_analytics_dimensions,
                                                   search_type)

        for filter_group in self.filter_groups:
            return self._get_search_analytics_data(gsc_client, date_from, date_to, search_analytics_dimensions,
                                                   search_type, filter_group=filter_group)

    def _get_search_analytics_data(self, gsc_client: GoogleSearchConsoleClient, date_from: date, date_to: date,
                                   search_analytics_dimensions: List[str], search_type: str,
                                   filter_group=None) -> Generator:
        if filter_group is None:
            filter_group = []
        try:
            paged_data = gsc_client.get_search_analytics_data(date_from, date_to, self.domain,
                                                              search_analytics_dimensions, search_type,
                                                              filter_group)
            return paged_data
        except ClientError as client_error:
            raise UserException(client_error.args[0].error_details[0]["message"]) from client_error

    @staticmethod
    def filter_duplicates_from_data(data: List[Dict]) -> List[Dict]:
        seen = set()
        new_data = []
        for datum in data:
            t = tuple(sorted(datum.items()))
            if t not in seen:
                seen.add(t)
                new_data.append(datum)
        return new_data

    @staticmethod
    def parse_list_from_string(string_list: str) -> List[str]:
        if "," in string_list:
            return [word.strip() for word in string_list.split(",") if len(word) > 1]
        else:
            return [string_list]

    def parse_search_analytics_data(self, data: List[Dict], dimensions: List[str]) -> List[Dict]:
        parsed_data = []
        for row in data:
            parsed_data.append(self._parse_search_analytics_row(row, dimensions))
        return parsed_data

    @staticmethod
    def _parse_search_analytics_row(row: Dict, dimensions: List[str]) -> Dict:
        parsed_row = {}
        for i, dimension in enumerate(dimensions):
            parsed_row[dimension] = row["keys"][i]
        data_headers = list(row.keys())
        data_headers.remove("keys")
        for key in data_headers:
            parsed_row[key] = row[key]
        return parsed_row

    def get_sitemaps_data(self, gsc_client: GoogleSearchConsoleClient) -> List[Dict]:
        logging.info("Fetching sitemaps data")
        data = self._get_sitemaps_data(gsc_client)
        logging.info("Parsing results")
        data = self.parse_sitemaps_data(data)
        return data

    def _get_sitemaps_data(self, gsc_client: GoogleSearchConsoleClient) -> List[Dict]:
        try:
            return gsc_client.get_sitemaps_data(self.domain)
        except ClientError as client_error:
            raise UserException(client_error.args[0].error_details[0]["message"]) from client_error
        except ClientAuthError as client_auth_error:
            raise UserException(client_auth_error)

    def parse_sitemaps_data(self, data: List[Dict]) -> List[Dict]:
        parsed_data = []
        for row in data:
            parsed_data.extend(self.parse_sitemaps_row(row))
        return parsed_data

    def parse_sitemaps_row(self, row: Dict) -> List[Dict]:
        if "contents" in row:
            return self._parse_sitemap_content_row(row)
        else:
            return self._parse_sitemap_error_row(row)

    @staticmethod
    def _parse_sitemap_content_row(row: Dict) -> List[Dict]:
        content_rows = []
        for content in row["contents"]:
            parsed_row = {}
            for sitemap_header in SITEMAPS_HEADERS:
                parsed_row[sitemap_header] = row.get(sitemap_header)
            parsed_row["content_type"] = content["type"]
            parsed_row["submitted"] = content["submitted"]
            parsed_row["indexed"] = content["indexed"]
            content_rows.append(parsed_row)
        return content_rows

    @staticmethod
    def _parse_sitemap_error_row(row: Dict) -> List[Dict]:
        error_rows = []
        parsed_row = {}
        for sitemap_header in SITEMAPS_HEADERS:
            parsed_row[sitemap_header] = row.get(sitemap_header)
        parsed_row["content_type"] = ""
        parsed_row["submitted"] = ""
        parsed_row["indexed"] = ""
        error_rows.append(parsed_row)
        return error_rows

    def get_date_range(self, date_from: str, date_to: str, date_range: str) -> Tuple[date, date]:
        if date_range == "Last week (sun-sat)":
            start_date, end_date = self.get_last_week_dates()
        elif date_range == "Last month":
            start_date, end_date = self.get_last_month_dates()
        elif date_range == "Custom":
            try:
                start_date = dateparser.parse(date_from).date()
                end_date = dateparser.parse(date_to).date()
            except AttributeError:
                raise UserException("Date input is invalid, please recheck the documentation on valid inputs")
        else:
            raise UserException(f"Date range type : {date_range} is invalid")
        return start_date, end_date

    @staticmethod
    def get_last_week_dates() -> Tuple[date, date]:
        today = date.today()
        offset = (today.weekday() - 5) % 7
        last_week_saturday = today - timedelta(days=offset)
        last_week_sunday = last_week_saturday - timedelta(days=6)
        return last_week_sunday, last_week_saturday

    @staticmethod
    def get_last_month_dates() -> Tuple[date, date]:
        last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
        start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
        return start_day_of_prev_month, last_day_of_prev_month

    @staticmethod
    def validate_table_name(table_name: str) -> None:
        if not table_name.replace("_", "").isalnum():
            raise UserException(
                "Output Table name is not valid, make sure it only contains alphanumeric characters and underscores")


if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
