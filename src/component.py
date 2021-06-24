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
endpoints = ['Search analytics', "Sitemaps"]

KEY_SEARCH_ANALYTICS_DIMENSIONS = "search_analytics_dimensions"
search_analytics_dimensionss = ['country', 'device', 'page', 'query', 'searchAppearance', 'date']

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
        domain = params.get(KEY_DOMAIN)
        if "sc-domain:" not in domain:
            domain = "".join(["sc-domain:", domain])

        data = []
        fieldnames = []
        if endpoint == "Search analytics":
            data, fieldnames = self.get_search_analytics_data(params, gsc_client, domain)
        elif endpoint == "Sitemaps":
            data, fieldnames = self.get_sitemaps_data(gsc_client, domain)
        elif endpoint == "Sites":
            data, fieldnames = self.get_sites_data(gsc_client, domain)

        if data:
            fieldnames.append("date_downloaded")
            date_downloaded = date.today()
            out_table = self.create_out_table_definition(name=out_table_name, columns=fieldnames)
            self.write_results(out_table, data, date_downloaded)
            self.write_tabledef_manifest(out_table)

    def get_search_analytics_data(self, params, gsc_client, domain):
        search_analytics_dimensions = params.get(KEY_SEARCH_ANALYTICS_DIMENSIONS)
        search_analytics_dimensions = self.parse_list_from_string(search_analytics_dimensions)
        logging.info(f"Fetching data for search analytics for {search_analytics_dimensions} dimensions")
        date_from = params.get(KEY_DATE_FROM)
        date_to = params.get(KEY_DATE_TO)
        date_range = params.get(KEY_DATE_RANGE)
        date_from, date_to = self.get_date_range(date_from, date_to, date_range)
        try:
            data = gsc_client.get_search_analytics_data(date_from, date_to, domain, search_analytics_dimensions)
        except ClientError as client_error:
            raise UserException(client_error) from client_error
        logging.info("Parsing results")
        if data:
            data, fieldnames = self.parse_search_analytics_data(data, search_analytics_dimensions)
            return data, fieldnames

    @staticmethod
    def get_gsc_client(client_id_credentials):
        client_id = client_id_credentials[KEY_CLIENT_ID]
        client_secret = client_id_credentials[KEY_CLIENT_SECRET]
        refresh_token = client_id_credentials[KEY_AUTH_DATA][KEY_REFRESH_TOKEN]
        return GoogleSearchConsoleClient(client_id, client_secret, refresh_token, TOKEN_URI)

    @staticmethod
    def parse_list_from_string(string_list):
        list = string_list.split(",")
        list = [word.strip() for word in list]
        return list

    def get_date_range(self, date_from, date_to, date_range):
        if date_range == "Last week (sun-sat)":
            date_from, date_to = self.get_last_week_dates()
        elif date_range == "Last month":
            date_from, date_to = self.get_last_month_dates()
        elif date_range == "Custom":
            date_from = dateparser.parse(date_from).date()
            date_to = dateparser.parse(date_to).date()
        return date_from, date_to

    @staticmethod
    def get_last_week_dates():
        today = date.today()
        offset = (today.weekday() - 5) % 7
        last_week_saturday = today - timedelta(days=offset)
        last_week_sunday = last_week_saturday - timedelta(days=6)
        return last_week_sunday, last_week_saturday

    def get_last_month_dates(self):
        last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
        start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
        return start_day_of_prev_month, last_day_of_prev_month

    def parse_search_analytics_data(self, data, dimensions):
        parsed_data = []
        keys = []
        for row in data:
            new_row = {}
            for i, dimension in enumerate(dimensions):
                new_row[dimension] = row["keys"][i]
            if not keys:
                del row['keys']
                result_keys = list(row.keys())
            for key in result_keys:
                new_row[key] = row[key]
            parsed_data.append(new_row)
        fieldnames = list(parsed_data[0].keys())
        return parsed_data, fieldnames

    @staticmethod
    def write_results(out_table, data, date_downloaded):
        with open(out_table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, out_table.columns)
            for result in data:
                result["date_downloaded"] = date_downloaded
                writer.writerow(result)

    def get_sitemaps_data(self, gsc_client, domain):
        try:
            data = gsc_client.get_sitemaps_data(domain)
        except ClientError as client_error:
            raise UserException(client_error) from client_error
        logging.info("Parsing results")
        data, fieldnames = self.parse_sitemaps_data(data)
        return data, fieldnames

    @staticmethod
    def parse_sitemaps_data(data):
        parsed_data = []
        for datum in data:
            if "contents" in datum:
                for content in datum["contents"]:
                    parsed_datum = {}
                    for sitemap_header in SITEMAPS_HEADERS:
                        parsed_datum[sitemap_header] = datum.get(sitemap_header)
                    parsed_datum["content_type"] = content["type"]
                    parsed_datum["submitted"] = content["submitted"]
                    parsed_datum["indexed"] = content["indexed"]
                    parsed_data.append(parsed_datum)
            else:
                parsed_datum = {}
                for sitemap_header in SITEMAPS_HEADERS:
                    parsed_datum[sitemap_header] = datum.get(sitemap_header)
                parsed_datum["content_type"] = ""
                parsed_datum["submitted"] = ""
                parsed_datum["indexed"] = ""
                parsed_data.append(parsed_datum)
        fieldnames = list(parsed_data[0].keys())
        return parsed_data, fieldnames

    def parse_sites_data(self, data):
        fieldnames = []
        return data, fieldnames


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
