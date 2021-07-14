import logging
import dateparser
import csv
from datetime import date
from datetime import timedelta
from keboola.component.base import ComponentBase, UserException
from google_search_console.client import GoogleSearchConsoleClient, ClientError

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
KEY_AUTH_DATA = "data"
TOKEN_URI = "https://oauth2.googleapis.com/token"

SITEMAPS_HEADERS = ["path", "lastSubmitted", "isPending", "isSitemapsIndex", "type", "lastDownloaded", "warnings",
                    "errors"]

REQUIRED_PARAMETERS = [KEY_DOMAIN, KEY_OUT_TABLE_NAME, KEY_ENDPOINT, KEY_SEARCH_ANALYTICS_DIMENSIONS]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self):
        params = self.configuration.parameters
        client_id_credentials = self.configuration.oauth_credentials
        gsc_client = self.get_gsc_client(client_id_credentials)
        out_table_name = params.get(KEY_OUT_TABLE_NAME)
        endpoint = params.get(KEY_ENDPOINT)
        domain = self.set_domain(params.get(KEY_DOMAIN))
        data, fieldnames = self.fetch_endpoint_data(endpoint, params, gsc_client, domain)
        if data:
            self.write_results(out_table_name, data, fieldnames)
        else:
            logging.warning("No data found!")

    @staticmethod
    def get_gsc_client(client_id_credentials):
        client_id = client_id_credentials[KEY_CLIENT_ID]
        client_secret = client_id_credentials[KEY_CLIENT_SECRET]
        refresh_token = client_id_credentials[KEY_AUTH_DATA][KEY_REFRESH_TOKEN]
        try:
            return GoogleSearchConsoleClient(client_id, client_secret, refresh_token, TOKEN_URI)
        except ClientError as client_error:
            raise UserException(client_error) from client_error

    @staticmethod
    def set_domain(domain):
        if "sc-domain:" not in domain:
            domain = "".join(["sc-domain:", domain])
        return domain

    def fetch_endpoint_data(self, endpoint, params, gsc_client, domain):
        if endpoint == "Search analytics":
            data, fieldnames = self.get_search_analytics_data(params, gsc_client, domain)
        elif endpoint == "Sitemaps":
            data, fieldnames = self.get_sitemaps_data(gsc_client, domain)
        else:
            raise ValueError("Endpoint selected does not exist")
        return data, fieldnames

    def write_results(self, out_table_name, data, fieldnames):
        fieldnames.append("date_downloaded")
        date_downloaded = date.today()
        out_table = self.create_out_table_definition(name=out_table_name, columns=fieldnames)
        self.write_results_to_out_table(out_table, data, date_downloaded)
        self.write_tabledef_manifest(out_table)

    @staticmethod
    def write_results_to_out_table(out_table, data, date_downloaded):
        with open(out_table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, out_table.columns)
            for result in data:
                result["date_downloaded"] = date_downloaded
                writer.writerow(result)

    def get_search_analytics_data(self, params, gsc_client, domain):
        search_analytics_dimensions = self.parse_list_from_string(params.get(KEY_SEARCH_ANALYTICS_DIMENSIONS))
        logging.info(f"Fetching data for search analytics for {search_analytics_dimensions} dimensions")
        date_from, date_to = self.get_date_range(params.get(KEY_DATE_FROM),
                                                 params.get(KEY_DATE_TO),
                                                 params.get(KEY_DATE_RANGE))
        data = self._get_search_analytics_data(gsc_client, date_from, date_to, domain, search_analytics_dimensions)
        logging.info("Parsing results")
        if data:
            data, fieldnames = self.parse_search_analytics_data(data, search_analytics_dimensions)
            return data, fieldnames

    @staticmethod
    def _get_search_analytics_data(gsc_client, date_from, date_to, domain, search_analytics_dimensions):
        try:
            return gsc_client.get_search_analytics_data(date_from, date_to, domain, search_analytics_dimensions)
        except ClientError as client_error:
            raise UserException(client_error) from client_error

    @staticmethod
    def parse_list_from_string(string_list):
        return [word.strip() for word in string_list.split(",")]

    def parse_search_analytics_data(self, data, dimensions):
        parsed_data = []
        fieldnames = []
        for row in data:
            parsed_data.append(self._parse_search_analytics_row(row, dimensions))
        if len(parsed_data) > 0:
            fieldnames = list(parsed_data[0].keys())
        return parsed_data, fieldnames

    @staticmethod
    def _parse_search_analytics_row(row, dimensions):
        parsed_row = {}
        for i, dimension in enumerate(dimensions):
            parsed_row[dimension] = row["keys"][i]
        data_headers = list(row.keys())
        data_headers.remove("keys")
        for key in data_headers:
            parsed_row[key] = row[key]
        return parsed_row

    def get_sitemaps_data(self, gsc_client, domain):
        logging.info("Fetching sitemaps data")
        data = self._get_sitemaps_data(gsc_client, domain)
        logging.info("Parsing results")
        data, fieldnames = self.parse_sitemaps_data(data)
        return data, fieldnames

    @staticmethod
    def _get_sitemaps_data(gsc_client, domain):
        try:
            return gsc_client.get_sitemaps_data(domain)
        except ClientError as client_error:
            raise UserException(client_error) from client_error

    def parse_sitemaps_data(self, data):
        parsed_data = []
        for row in data:
            parsed_data.extend(self.parse_sitemaps_row(row))
        fieldnames = list(parsed_data[0].keys())
        return parsed_data, fieldnames

    def parse_sitemaps_row(self, row):
        if "contents" in row:
            return self._parse_sitemap_content_row(row)
        else:
            return self._parse_sitemap_error_row(row)

    @staticmethod
    def _parse_sitemap_content_row(row):
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
    def _parse_sitemap_error_row(row):
        error_rows = []
        parsed_row = {}
        for sitemap_header in SITEMAPS_HEADERS:
            parsed_row[sitemap_header] = row.get(sitemap_header)
        parsed_row["content_type"] = ""
        parsed_row["submitted"] = ""
        parsed_row["indexed"] = ""
        error_rows.append(parsed_row)
        return error_rows

    def get_date_range(self, date_from, date_to, date_range):
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
    def get_last_week_dates():
        today = date.today()
        offset = (today.weekday() - 5) % 7
        last_week_saturday = today - timedelta(days=offset)
        last_week_sunday = last_week_saturday - timedelta(days=6)
        return last_week_sunday, last_week_saturday

    @staticmethod
    def get_last_month_dates():
        last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
        start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
        return start_day_of_prev_month, last_day_of_prev_month


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
