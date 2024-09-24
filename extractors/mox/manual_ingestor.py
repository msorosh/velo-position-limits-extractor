from abstract_extractor import AbstractExtractor
from utils_mox import MoxNoxAbstractExtractor
import boto3
import pandas as pd

class MoxExtractor(AbstractExtractor):
    def __init__(self):
        self.url_iceblock = "https://www.ice.com/publicdocs/futures/ICEBlock_Equity_contract_list.xlsx"
        self.prefix_nox = "nox"
        self.s3_client = boto3.client("s3")
        self.input_bucket_name = "s3-ingested-raw-data-dev"
        self.out_bucket_name = "s3-postprocess-data-dev"
        self.sheet_id_nox = "1-AR-8pNEtfSangJ4pHxsj52TbUig9rUN"
        self.sheet_id_mox = "1A9JX0zH-GC6AlvoWDb9NxDEAM52HLBm2"

    def extract(self):
        mox_spreadsheet_dict = pd.read_excel(f"https://docs.google.com/spreadsheets/d/{self.sheet_id_mox}/export?&format=xlsx",
                                             sheet_name=None, header=0, skiprows=0, engine='openpyxl', keep_default_na = False)
        for sheet_name, df_exchange in mox_spreadsheet_dict.items():
            df_exchange = AbstractExtractor.manual_fill_na(df_exchange)
            AbstractExtractor.save_df_to_s3(df_exchange, self.out_bucket_name, sheet_name)
        # Extract no limit exchanges
        nox_spreadsheet_df = pd.read_excel(
            f"https://docs.google.com/spreadsheets/d/{self.sheet_id_nox}/export?&format=xlsx",
            sheet_name="no_limit_all", header=0, skiprows=0, engine='openpyxl', keep_default_na = False)
        nox_spreadsheet_df = nox_spreadsheet_df.fillna("")
        # df_temp = MoxNoxAbstractExtractor().load_nox_data(self.input_bucket_name, self.prefix_nox)
        for col in nox_spreadsheet_df.columns:
            if col not in ["PRODUCT_CODE"]:
                if "_MONTH_LIMIT_VALUE_LEG" in col:
                    nox_spreadsheet_df[col] = nox_spreadsheet_df[col].fillna(-1)
                else:
                    nox_spreadsheet_df[col] = nox_spreadsheet_df[col].fillna('')
        AbstractExtractor.save_df_to_s3(nox_spreadsheet_df, self.out_bucket_name, self.prefix_nox)

        prefix = "ICEBLOCK"
        df_combined = MoxNoxAbstractExtractor().get_iceblock_products(self.url_iceblock)
        AbstractExtractor.save_df_to_s3(df_combined, self.out_bucket_name, prefix)
