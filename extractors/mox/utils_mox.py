storage_options = {'User-Agent': 'Mozilla/5.0'}
import boto3
import pandas as pd
import io


class MoxNoxAbstractExtractor:
    def __init__(self):
        self.s3_client = boto3.client("s3")

    @staticmethod
    def load_sso(url):
        sso_df = pd.read_excel(url, sheet_name="Single Stock Option",
                               header=[2, 3, 4, 5], storage_options=storage_options)
        sso_df.columns = [f"{x[0].strip()} {x[1].strip()} {x[2].strip()} {x[3].strip()}" for x in sso_df.columns]
        sso_df = sso_df[[
            'Fees: Unnamed: 1_level_1 Underlying Company Name',
            'Product codes Flexible contracts American style options Cash',
            'Product codes Flexible contracts American style options Physical',
            'Product codes Flexible contracts European style Options Cash',
            'Product codes Flexible contracts European style Options Physical',
            'Product codes Standard contracts Standard Central Order Book Contracts London IEO - American  Style Physical',
            'Flexible contract specification Unnamed: 17_level_1 Unnamed: 17_level_2 Contract Size (shares per lot)',
            'Standard contract specification Unnamed: 29_level_1 Unnamed: 29_level_2 Contract Size (shares per lot)',

        ]]
        sso_df.columns = ["product_name",
                          "flex_american_cash", "flex_american_physical",
                          "flex_european_cash", "flex_european_physical",
                          "standard_american_physical",
                          "flex_contract_size",
                          "standard_contract_size"]
        # Load standard american physical
        sso_df_standard = sso_df[~sso_df["standard_american_physical"].isna()]
        sso_df_standard["product_name"] = sso_df_standard["product_name"].astype(
            str) + " Standard American Style Physical Options"
        sso_df_standard["product_code"] = sso_df_standard["standard_american_physical"]
        sso_df_standard["settlement_type"] = "PHYSICAL"
        sso_df_standard["product_type"] = "Options"
        sso_df_standard["product_group"] = "SSO"
        sso_df_standard["contract_size"] = sso_df_standard["standard_contract_size"]
        sso_df_standard = sso_df_standard[
            ["product_name", "product_code", "settlement_type", "product_type", "product_group", "contract_size"]]

        # Load flex american physical
        sso_df_flex_american_physical = sso_df[~sso_df["flex_american_physical"].isna()]
        sso_df_flex_american_physical["product_name"] = sso_df_flex_american_physical["product_name"].astype(
            str) + " Flex American Style Physical Options"
        sso_df_flex_american_physical["product_code"] = sso_df_flex_american_physical["flex_american_physical"]
        sso_df_flex_american_physical["settlement_type"] = "PHYSICAL"
        sso_df_flex_american_physical["product_type"] = "Options"
        sso_df_flex_american_physical["product_group"] = "SSO"
        sso_df_flex_american_physical["contract_size"] = sso_df_flex_american_physical["flex_contract_size"]
        sso_df_flex_american_physical = sso_df_flex_american_physical[
            ["product_name", "product_code", "settlement_type", "product_type", "product_group", "contract_size"]]

        # Load flex american cash
        sso_df_flex_american_cash = sso_df[~sso_df["flex_american_cash"].isna()]
        sso_df_flex_american_cash["product_name"] = sso_df_flex_american_cash["product_name"].astype(
            str) + " Flex American Style Cash Options"
        sso_df_flex_american_cash["product_code"] = sso_df_flex_american_cash["flex_american_cash"]
        sso_df_flex_american_cash["settlement_type"] = "CASH"
        sso_df_flex_american_cash["product_type"] = "Options"
        sso_df_flex_american_cash["product_group"] = "SSO"
        sso_df_flex_american_cash["contract_size"] = sso_df_flex_american_cash["flex_contract_size"]
        sso_df_flex_american_cash = sso_df_flex_american_cash[
            ["product_name", "product_code", "settlement_type", "product_type", "product_group", "contract_size"]]

        # Load flex european  physical
        sso_df_flex_european_physical = sso_df[~sso_df["flex_european_physical"].isna()]
        sso_df_flex_european_physical["product_name"] = sso_df_flex_european_physical["product_name"].astype(
            str) + " Flex European Style Physical Options"
        sso_df_flex_european_physical["product_code"] = sso_df_flex_european_physical["flex_european_physical"]
        sso_df_flex_european_physical["settlement_type"] = "PHYSICAL"
        sso_df_flex_european_physical["product_type"] = "Options"
        sso_df_flex_european_physical["product_group"] = "SSO"
        sso_df_flex_european_physical["contract_size"] = sso_df_flex_european_physical["flex_contract_size"]
        sso_df_flex_european_physical = sso_df_flex_european_physical[
            ["product_name", "product_code", "settlement_type", "product_type", "product_group", "contract_size"]]

        # Load flex european  cash
        sso_df_flex_european_cash = sso_df[~sso_df["flex_european_cash"].isna()]
        sso_df_flex_european_cash["product_name"] = sso_df_flex_european_cash["product_name"].astype(
            str) + " Flex European Style Cash Options"
        sso_df_flex_european_cash["product_code"] = sso_df_flex_european_cash["flex_european_cash"]
        sso_df_flex_european_cash["settlement_type"] = "CASH"
        sso_df_flex_european_cash["product_type"] = "Options"
        sso_df_flex_european_cash["product_group"] = "SSO"
        sso_df_flex_european_cash["contract_size"] = sso_df_flex_european_cash["flex_contract_size"]
        sso_df_flex_european_cash = sso_df_flex_european_cash[
            ["product_name", "product_code", "settlement_type", "product_type", "product_group", "contract_size"]]
        sso_df = pd.concat(
            [sso_df_standard, sso_df_flex_american_physical, sso_df_flex_american_cash, sso_df_flex_european_physical,
             sso_df_flex_european_cash], ignore_index=True)
        return sso_df

    @staticmethod
    def load_ssf(url):
        ssf_df = pd.read_excel(url, sheet_name="Single Stock Futures", header=[3, 4, 5],
                               storage_options=storage_options)
        ssf_df.columns = [f"{x[0].strip()} {x[1].strip()} {x[2].strip()}" for x in ssf_df.columns]
        ssf_df = ssf_df[["Unnamed: 1_level_0 Underlying Company Name",
                         "Product codes Flexible contracts Cash",
                         "Product codes Flexible contracts Physical",
                         "Product codes Standard contracts Contract code",
                         "Flexible contract specification Unnamed: 16_level_1 Contract Size (shares per lot)",
                         "Standard contract specification Unnamed: 23_level_1 Contract Size (shares per lot)"]]
        ssf_df.columns = ["product_name",
                          "flex_cash", "flex_physical",
                          "standard",
                          "flex_contract_size",
                          "standard_contract_size"]

        # Load standard
        ssf_df_standard = ssf_df[~ssf_df["standard"].isna()]
        ssf_df_standard["product_name"] = ssf_df_standard["product_name"].astype(
            str) + " Standard Futures"
        ssf_df_standard["product_code"] = ssf_df_standard["standard"]
        ssf_df_standard["settlement_type"] = ""
        ssf_df_standard["product_type"] = "Futures"
        ssf_df_standard["product_group"] = "SSF"
        ssf_df_standard["contract_size"] = ssf_df_standard["standard_contract_size"]
        ssf_df_standard = ssf_df_standard[["product_name", "product_code", "settlement_type", "product_type",
                                           "product_group", "contract_size"]]

        # Load flex physical
        ssf_df_flex_physical = ssf_df[~ssf_df["flex_physical"].isna()]
        ssf_df_flex_physical["product_name"] = ssf_df_flex_physical["product_name"].astype(
            str) + " Flex Physical Futures"
        ssf_df_flex_physical["product_code"] = ssf_df_flex_physical["flex_physical"]
        ssf_df_flex_physical["settlement_type"] = "PHYSICAL"
        ssf_df_flex_physical["product_type"] = "Futures"
        ssf_df_flex_physical["product_group"] = "SSF"
        ssf_df_flex_physical["contract_size"] = ssf_df_flex_physical["flex_contract_size"]
        ssf_df_flex_physical = ssf_df_flex_physical[["product_name", "product_code", "settlement_type",
                                                     "product_type", "product_group", "contract_size"]]

        # Load flex cash
        ssf_df_flex_cash = ssf_df[~ssf_df["flex_cash"].isna()]
        ssf_df_flex_cash["product_name"] = ssf_df_flex_cash["product_name"].astype(str) + " Flex Cash Futures"
        ssf_df_flex_cash["product_code"] = ssf_df_flex_cash["flex_cash"]
        ssf_df_flex_cash["settlement_type"] = "CASH"
        ssf_df_flex_cash["product_type"] = "Futures"
        ssf_df_flex_cash["product_group"] = "SSF"
        ssf_df_flex_cash["contract_size"] = ssf_df_flex_cash["flex_contract_size"]
        ssf_df_flex_cash = ssf_df_flex_cash[
            ["product_name", "product_code", "settlement_type", "product_type", "product_group", "contract_size"]]

        ssf_df = pd.concat(
            [ssf_df_standard, ssf_df_flex_physical, ssf_df_flex_cash], ignore_index=True)
        return ssf_df

    @staticmethod
    def load_index(url):
        index_df = pd.read_excel(url, sheet_name="Index Futures and Options",
                                 header=4, storage_options=storage_options)
        index_df.columns = [x.strip() for x in index_df.columns]
        index_df = index_df[["Index Name", "Flexible European Style Options", "Standard Futures Contract"]]
        index_df.columns = ["product_name", "flex_european_options", "standard_futures"]

        # Load standard
        index_df_futures = index_df[~index_df["standard_futures"].isna()]
        index_df_futures["product_name"] = index_df_futures["product_name"].astype(
            str) + " Standard Futures"
        index_df_futures["product_code"] = index_df_futures["standard_futures"]
        index_df_futures["settlement_type"] = ""
        index_df_futures["product_type"] = "Futures"
        index_df_futures["product_group"] = "Index"
        index_df_futures["contract_size"] = ""
        index_df_futures = index_df_futures[
            ["product_name", "product_code", "product_type", "product_group", "contract_size"]]

        # Load flex european option
        index_df_options = index_df[~index_df["flex_european_options"].isna()]
        index_df_options["product_name"] = index_df_options["product_name"].astype(
            str) + " Flex European style Options"
        index_df_options["product_code"] = index_df_options["flex_european_options"]
        index_df_options["settlement_type"] = ""
        index_df_options["product_type"] = "Options"
        index_df_options["product_group"] = "Index"
        index_df_options["contract_size"] = ""
        index_df_options = index_df_options[
            ["product_name", "product_code", "product_type", "product_group", "contract_size"]]

        ssf_index = pd.concat(
            [index_df_futures, index_df_options], ignore_index=True)
        return ssf_index

    @staticmethod
    def load_dividend(url):
        dividend_df = pd.read_excel(url, sheet_name="Dividend Adjusted Stock Futures",
                                    header=5, storage_options=storage_options)
        dividend_df.columns = [x.strip() for x in dividend_df.columns]
        dividend_df = dividend_df[["Company Name", "Cash", "Physical", "Contract Size (shares per lot)"]]
        dividend_df.columns = ["product_name", "Cash", "Physical", "contract_size"]

        # Load standard
        dividend_df_cash = dividend_df[~dividend_df["Cash"].isna()]
        dividend_df_cash["product_name"] = dividend_df_cash["product_name"].astype(
            str) + " Dividend Cash Futures"
        dividend_df_cash["product_code"] = dividend_df_cash["Cash"]
        dividend_df_cash["settlement_type"] = ""
        dividend_df_cash["product_type"] = "Futures"
        dividend_df_cash["product_group"] = "Dividend"
        dividend_df_cash["contract_size"] = dividend_df_cash["contract_size"]
        dividend_df_cash = dividend_df_cash[
            ["product_name", "product_code", "product_type", "product_group", "contract_size"]]

        # Load flex european option
        dividend_df_physical = dividend_df[~dividend_df["Physical"].isna()]
        dividend_df_physical["product_name"] = dividend_df_physical["product_name"].astype(
            str) + " Dividend Physical Futures"
        dividend_df_physical["product_code"] = dividend_df_physical["Physical"]
        dividend_df_physical["settlement_type"] = ""
        dividend_df_physical["product_type"] = "Futures"
        dividend_df_physical["product_group"] = "Dividend"
        dividend_df_physical["contract_size"] = dividend_df_physical["contract_size"]
        dividend_df_physical = dividend_df_physical[
            ["product_name", "product_code", "product_type", "product_group", "contract_size"]]

        dividend_df = pd.concat([dividend_df_cash, dividend_df_physical], ignore_index=True)
        return dividend_df

    def get_iceblock_products(self, url):
        df_combined = pd.concat([self.load_sso(url), self.load_ssf(url), self.load_index(url),
                                 self.load_dividend(url)], ignore_index=True)
        df_combined = df_combined.fillna('')
        df_combined["EXCHANGE_NAME"] = "ICEBLOCK"
        df_combined["EXCHANGE_CODE"] = "ICEBLOCK"
        df_combined = df_combined.rename(columns={"product_name": "PRODUCT_NAME", "product_code": "PRODUCT_CODE",
                                                  "settlement_type": "SETTLEMENT_TYPE", "product_type": "PRODUCT_TYPE",
                                                  "product_group": "PRODUCT_TYPE",
                                                  "contract_size": "CONTRACT_SIZE"})
        df_combined = df_combined[df_combined.PRODUCT_CODE.apply(lambda x: True if len(x.strip()) else False)]
        return df_combined

    def load_nox_data(self, bucket_name, prefix):
        objs = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix}/", Delimiter="/")['Contents']
        objs_sorted = sorted(objs, key=lambda x: x['LastModified'], reverse=True)
        objs_sorted = [x for x in objs_sorted if '.xlsx' in x["Key"]]
        obj_latest = self.s3_client.get_object(Bucket=bucket_name, Key=objs_sorted[0]['Key'])
        df_latest = pd.read_excel(io.BytesIO(obj_latest['Body'].read()), sheet_name="no_limit_all", engine='openpyxl',
                                  keep_default_na=False)
        df_latest = df_latest.fillna("")
        return df_latest
