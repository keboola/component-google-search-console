import logging
import dateparser
import csv
from datetime import date
from datetime import timedelta
from typing import List
from keboola.component.base import ComponentBase, UserException
from google_search_console import GoogleSearchConsoleClient, ClientError
from keboola.component.dao import OauthCredentials
from typing import Dict
from typing import Tuple
from keboola.component.dao import TableDefinition

KEY_DOMAIN = 'domain'
KEY_OUT_TABLE_NAME = "out_table_name"
KEY_ENDPOINT = "endpoint"
KEY_SEARCH_ANALYTICS_DIMENSIONS = "search_analytics_dimensions"
KEY_DATE_FROM = "date_from"
KEY_DATE_TO = "date_to"
KEY_DATE_RANGE = "date_range"
KEY_CLIENT_ID = "appKey"
KEY_CLIENT_SECRET = "appSecret"
KEY_REFRESH_TOKEN = "refresh_token"
KEY_FILTER_GROUPS = "filter_groups"
KEY_AUTH_DATA = "data"

# TODO: this should be part of the client module and token_uri should use it as default parameter
# is it ever going to change?
TOKEN_URI = "https://oauth2.googleapis.com/token"

SITEMAPS_HEADERS = ["path", "lastSubmitted", "isPending", "isSitemapsIndex", "type", "lastDownloaded", "warnings",
                    "errors"]

REQUIRED_PARAMETERS = [KEY_DOMAIN, KEY_OUT_TABLE_NAME, KEY_ENDPOINT]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    def __init__(self) -> None:
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)
        self.endpoint: str = ""
        self.domain: str = ""
        self.filter_groups: List = []
        self.out_table_name: str = ""

    def run(self) -> None:
        params = self.configuration.parameters
        client_id_credentials = self.configuration.oauth_credentials
        gsc_client = self.get_gsc_client(client_id_credentials)
        # TODO: Any reason for these to be initialized outside the constructor?
        self.out_table_name = params.get(KEY_OUT_TABLE_NAME)
        self.validate_table_name(self.out_table_name)
        self.endpoint = params.get(KEY_ENDPOINT)
        self.domain = self.get_domain_string(params.get(KEY_DOMAIN))
        self.filter_groups = params.get(KEY_FILTER_GROUPS, [[]])
        data, fieldnames = self.fetch_endpoint_data(gsc_client)
        if data:
            self.write_results(data, fieldnames)
        else:
            logging.warning("No data found!")

    @staticmethod
    def get_gsc_client(client_id_credentials: OauthCredentials) -> GoogleSearchConsoleClient:
        if client_id_credentials:
            client_id = client_id_credentials[KEY_CLIENT_ID]
            client_secret = client_id_credentials[KEY_CLIENT_SECRET]
            refresh_token = client_id_credentials[KEY_AUTH_DATA][KEY_REFRESH_TOKEN]
        else:
            raise UserException(
                "Component is not authorized, please authorize the app in the authorization configuration ")
        try:
            return GoogleSearchConsoleClient(client_id, client_secret, refresh_token, TOKEN_URI)
        except ClientError as client_error:
            raise UserException(client_error) from client_error

    @staticmethod
    def get_domain_string(domain: str) -> str:
        if "sc-domain:" not in domain:
            domain = "".join(["sc-domain:", domain])
        return domain

    def fetch_endpoint_data(self, gsc_client: GoogleSearchConsoleClient) -> Tuple[List[Dict], List[str]]:
        # TODO: will this scale memory-wise? Is it save to process everything in-memory?

        if self.endpoint == "Search analytics":
            data, fieldnames = self.get_search_analytics_data(gsc_client)
        elif self.endpoint == "Sitemaps":
            data, fieldnames = self.get_sitemaps_data(gsc_client)
        else:
            raise ValueError("Endpoint selected does not exist")
        # TODO: Extracting fieldnames in both methods seems redundant, they're contained within the dictionary anyway
        # This could be handled on the writer side, even if the fields were not consistent
        return data, fieldnames

    def write_results(self, data: List[Dict], fieldnames: List[str]) -> None:
        fieldnames.append("date_downloaded")
        fieldnames.append("domain")
        date_downloaded = date.today()
        out_table = self.create_out_table_definition(name=self.out_table_name, columns=fieldnames)
        self.write_results_to_out_table(out_table, data, date_downloaded)
        self.write_tabledef_manifest(out_table)

    def write_results_to_out_table(self, out_table: TableDefinition, data: List[Dict], date_downloaded: date) -> None:
        with open(out_table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, out_table.columns)
            for result in data:
                result["date_downloaded"] = date_downloaded
                result["domain"] = self.domain
                writer.writerow(result)

    def get_search_analytics_data(self, gsc_client: GoogleSearchConsoleClient) -> Tuple[List[Dict], List[str]]:
        params = self.configuration.parameters
        search_analytics_dimensions = self.parse_list_from_string(params.get(KEY_SEARCH_ANALYTICS_DIMENSIONS))
        if not search_analytics_dimensions:
            raise UserException("Missing Search Analytics dimensions, please fill them in")

        logging.info(f"Fetching data for search analytics for {search_analytics_dimensions} dimensions")
        date_from, date_to = self.get_date_range(params.get(KEY_DATE_FROM),
                                                 params.get(KEY_DATE_TO),
                                                 params.get(KEY_DATE_RANGE))

        data = []
        fieldnames = []
        for filter_group in self.filter_groups:
            data.extend(self._get_search_analytics_data(gsc_client, date_from, date_to, search_analytics_dimensions,
                                                        filter_group))
        logging.info("Parsing results")
        if data:
            data, fieldnames = self.parse_search_analytics_data(data, search_analytics_dimensions)
            data = self.filter_duplicates_from_data(data)
        return data, fieldnames

    def _get_search_analytics_data(self, gsc_client: GoogleSearchConsoleClient, date_from: date, date_to: date,
                                   search_analytics_dimensions: List[str], filter_group: List[dict]) -> List[Dict]:
        try:
            data = gsc_client.get_search_analytics_data(date_from, date_to, self.domain, search_analytics_dimensions,
                                                        filter_group)
            return data
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
        return [word.strip() for word in string_list.split(",") if len(word) > 1]

    def parse_search_analytics_data(self, data: List[Dict], dimensions: List[str]) -> Tuple[List[Dict], List[str]]:
        parsed_data = []
        fieldnames = []
        for row in data:
            parsed_data.append(self._parse_search_analytics_row(row, dimensions))
        if len(parsed_data) > 0:
            fieldnames = list(parsed_data[0].keys())
        return parsed_data, fieldnames

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

    def get_sitemaps_data(self, gsc_client: GoogleSearchConsoleClient) -> Tuple[List[Dict], List[str]]:
        logging.info("Fetching sitemaps data")
        data = self._get_sitemaps_data(gsc_client)
        logging.info("Parsing results")
        data, fieldnames = self.parse_sitemaps_data(data)
        return data, fieldnames

    def _get_sitemaps_data(self, gsc_client: GoogleSearchConsoleClient) -> List[Dict]:
        try:
            return gsc_client.get_sitemaps_data(self.domain)
        except ClientError as client_error:
            raise UserException(client_error.args[0].error_details[0]["message"]) from client_error

    def parse_sitemaps_data(self, data: List[Dict]) -> Tuple[List[Dict], List[str]]:
        parsed_data = []
        for row in data:
            parsed_data.extend(self.parse_sitemaps_row(row))
        fieldnames = list(parsed_data[0].keys())
        return parsed_data, fieldnames

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
        # TODO: I don't like this much. input variables are reused and outputted in return statement holding completely
        # different types/things which is congnitively confusing. There is no validation of date_range parameter,
        # so if the users enters something not supported it will return
        # the original input variables date_* as return values
        if date_range == "Last week (sun-sat)":
            date_from, date_to = self.get_last_week_dates()
        elif date_range == "Last month":
            date_from, date_to = self.get_last_month_dates()
        elif date_range == "Custom":
            try:
                date_from = dateparser.parse(date_from).date()
                date_to = dateparser.parse(date_to).date()
            except AttributeError:
                raise UserException("Date input is invalid, please recheck the documentation on valid inputs")
        return date_from, date_to

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
