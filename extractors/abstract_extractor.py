import boto3
from io import StringIO, BytesIO
from datetime import datetime
from pathlib import Path
import pandas as pd

s3_client = boto3.client("s3")


class AbstractExtractor:
    @staticmethod
    def save_df_to_s3(df, bucket_name, exchange_name, merger=False):
        current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # convert datetime obj to string
        str_current_datetime = str(current_datetime)
        csv_buffer = StringIO()

        if exchange_name in ["all_exchanges_output", "reporting_table", "limit_table", "exchange_table",
                             "product_table",
                             "diminishing_table"]:
            filename = f'{exchange_name}_{str_current_datetime}.csv'
        else:
            filename = f'{exchange_name}/{exchange_name}_data_{str_current_datetime}.csv'
        df.to_csv(csv_buffer, index=merger)
        s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=filename)
        print(f"data for {exchange_name} successfully loaded on S3")

    @staticmethod
    def create_code_for_dupes2(df):
        df = AbstractExtractor.manual_fill_na(df)
        df = df[df["PRODUCT_CODE"] != ""]
        df["PRODUCT_CODE"] = df["PRODUCT_CODE"].astype(str)
        df = df.sort_values(by=["PRODUCT_CODE", "PRODUCT_NAME"], ascending=False)
        # df = df[df.PRODUCT_CODE.isin(["T", "N"])]
        # load product_list reference file
        objs = s3_client.list_objects_v2(Bucket="velo-unique-product-ids")['Contents']
        objs_sorted = sorted(objs, key=lambda x: x['LastModified'], reverse=True)
        output_latest = [x for x in objs_sorted if '.csv' in x["Key"]][0]
        reference_products_id_data = s3_client.get_object(Bucket="velo-unique-product-ids", Key=output_latest['Key'])
        reference_products_id_df = pd.read_csv(BytesIO(reference_products_id_data['Body'].read()), keep_default_na = False)
        # reference_products_id_df = pd.DataFrame(columns=["EXCHANGE_CODE", "PRODUCT_NAME", "PRODUCT_CODE"])

        reference_products_id_df["code_name"] = reference_products_id_df.apply(lambda x: str(x["EXCHANGE_CODE"]) + str(x["PRODUCT_NAME"]), axis=1)
        mapper = reference_products_id_df.set_index("code_name", drop=True).to_dict()["PRODUCT_CODE"]
        df["PRODUCT_CODE"] = df.apply(
            lambda x: mapper[x["EXCHANGE_CODE"] + x["PRODUCT_NAME"]] if x["EXCHANGE_CODE"] + x[
                "PRODUCT_NAME"] in mapper.keys() else x["PRODUCT_CODE"], axis=1)
        existing_codes_combo = reference_products_id_df.apply(
            lambda row: str(row["EXCHANGE_CODE"]) + "|" + str(row["PRODUCT_CODE"]), axis=1
        ).tolist()

        existing_codes_name_combo = reference_products_id_df.apply(
            lambda row: str(row["EXCHANGE_CODE"]) + "|" + str(row["PRODUCT_NAME"]) + "|" + str(row["PRODUCT_CODE"]), axis=1
        ).tolist()
        new_product_exchange_name_codes = []
        new_product_exchange_codes = []
        temp_n = []
        for index, row in df.iterrows():
            new_codes_combo = str(row["EXCHANGE_CODE"]) + "|" + str(row["PRODUCT_CODE"])
            new_codes_name_combo = str(row["EXCHANGE_CODE"]) + "|" + str(row["PRODUCT_NAME"]) + "|" + str(row["PRODUCT_CODE"])

            if new_codes_combo not in existing_codes_combo:
                new_product_exchange_name_codes.append(
                    [str(row["EXCHANGE_CODE"]), str(row["PRODUCT_NAME"]), str(row["PRODUCT_CODE"])])
                existing_codes_combo.append(new_codes_combo)
                existing_codes_name_combo.append(new_codes_name_combo)
                temp_n.append(new_codes_combo)

            elif new_codes_name_combo not in existing_codes_name_combo:
                    n = len([x for x in temp_n if x in new_codes_combo])
                    # df.loc[index, "PRODUCT_CODE"] = str(row["PRODUCT_CODE"]) + f"_{n + 1}"
                    new_product_exchange_name_codes.append([str(row["EXCHANGE_CODE"]), str(row["PRODUCT_NAME"]), str(row["PRODUCT_CODE"]) + f"_{n + 1}"])
                    existing_codes_name_combo.append(new_codes_name_combo)
                    existing_codes_combo.append(new_codes_combo)
                    temp_n.append(new_codes_combo)


        df_temp = pd.DataFrame(new_product_exchange_name_codes)
        if df_temp.shape[0] != 0:
            df_temp = df_temp.drop_duplicates(keep="first", ignore_index=True)
            df_temp.columns = ["EXCHANGE_CODE", "PRODUCT_NAME", "PRODUCT_CODE"]
            df_temp = pd.concat([df_temp, reference_products_id_df], ignore_index=True)
            current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            # convert datetime obj to string
            str_current_datetime = str(current_datetime)
            csv_buffer = StringIO()
            filename = f'unique_product_ids_{str_current_datetime}.csv'
            df_temp.to_csv(csv_buffer, index=False)
            s3_client.put_object(Body=csv_buffer.getvalue(), Bucket="velo-unique-product-ids", Key=filename)

            df_temp["code_name"] = df_temp.apply(lambda x: str(x["EXCHANGE_CODE"]) + str(x["PRODUCT_NAME"]), axis=1)
            mapper = df_temp.set_index("code_name", drop=True).to_dict()["PRODUCT_CODE"]

            df["PRODUCT_CODE"] = df.apply(
                lambda x: mapper[x["EXCHANGE_CODE"] + x["PRODUCT_NAME"]] if x["EXCHANGE_CODE"] + x["PRODUCT_NAME"] in mapper.keys() else x["PRODUCT_CODE"], axis=1)
        return df

    def manual_fill_na(df):
        for col in df.columns:
            if col not in ["PRODUCT_CODE"]:
                df[col] = df[col].fillna("")
        return df

