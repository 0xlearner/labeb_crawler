import httpx
import pandas as pd
import csv
import logging
from glob import glob
import os
import base64

# logging.basicConfig(
#     filename="post_script.log",
#     format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
#     filemode="w",
# )

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

csv.field_size_limit(100000000)

headers = {"Accept": "*/*", "Content-Type": "application/json"}


def get_directory(store_id):
    for dirpath, dirnames, files in os.walk(os.getcwd()):
        for folder in dirnames:
            if folder.startswith(store_id) and "images" in folder:
                print(folder)
                return folder


def encoded_images(dir, catalog_uuid):

    for d in os.listdir(dir):
        flist = list()
        cat_uuid = d.split("_")[-1]
        if cat_uuid == catalog_uuid:
            print(catalog_uuid)
            path = os.path.join(dir, d)
            for file in os.listdir(os.path.join(path)):
                print(file)
                with open(os.path.join(path, file), "rb") as image_file:
                    encoded_str = base64.b64encode(image_file.read())
                    li_encoded_str = "data:image/jpeg;base64," + encoded_str.decode(
                        "ascii"
                    )
                    flist.append(li_encoded_str)

            return flist


def post_func(client: httpx.Client, file):

    data = {}
    for chunk in pd.read_csv(file, chunksize=5):
        df = chunk.fillna("").astype(str)
        dict_df = df.to_dict("records")
        for row in dict_df:
            data.setdefault((row["LabebStoreId"], row["catalog_uuid"]), []).append(row)
    for payload_no, v in enumerate(data.values(), 1):
        if len(v) == 1:
            payload = {
                "row": {
                    "LabebStoreId": v[0]["LabebStoreId"],
                    "catalog_uuid": v[0]["catalog_uuid"],
                    "lang": v[0]["lang"],
                    "cat_0_name": v[0]["cat_0_name"],
                    "cat_1_name": v[0]["cat_1_name"],
                    "cat_2_name": v[0]["cat_2_name"],
                    "cat_3_name": v[0]["cat_3_name"],
                    "catalogname": v[0]["catalogname"],
                    "description": v[0]["description"],
                    "properties": v[0]["properties"],
                    "price": v[0]["price"],
                    "price_before_discount": v[0]["price_before_discount"],
                    "externallink": v[0]["externallink"],
                    "Rating": v[0]["Rating"],
                    "delivery": v[0]["delivery"],
                    "discount": v[0]["discount"],
                    "instock": v[0]["instock"],
                }
            }
            directory = get_directory(payload["row"]["LabebStoreId"])
            catalouge = v[0]["catalog_uuid"]
            imgs = encoded_images(directory, catalouge)

            payload["row"]["images"] = imgs
            try:
                print(f"""row: {len(payload["row"]["images"])}""")
            except Exception as e:
                logger.error(e)

            response = client.post(
                f"""http://crawlerapi.labeb.com/api/PCCrawler/Crawl?StoreId={v[0]["LabebStoreId"]}""",
                json=payload,
            )
            logger.debug(f"""row: {payload["row"]["externallink"]}""")
            logger.info(response.text)

        else:
            payload = {
                "row": {
                    "LabebStoreId": v[0]["LabebStoreId"],
                    "catalog_uuid": v[0]["catalog_uuid"],
                    "lang": v[0]["lang"],
                    "cat_0_name": v[0]["cat_0_name"],
                    "cat_1_name": v[0]["cat_1_name"],
                    "cat_2_name": v[0]["cat_2_name"],
                    "cat_3_name": v[0]["cat_3_name"],
                    "catalogname": v[0]["catalogname"],
                    "description": v[0]["description"],
                    "properties": v[0]["properties"],
                    "price": v[0]["price"],
                    "price_before_discount": v[0]["price_before_discount"],
                    "externallink": v[0]["externallink"],
                    "Rating": v[0]["Rating"],
                    "delivery": v[0]["delivery"],
                    "discount": v[0]["discount"],
                    "instock": v[0]["instock"],
                },
                "nextRow": {
                    "LabebStoreId": v[1]["LabebStoreId"],
                    "catalog_uuid": v[1]["catalog_uuid"],
                    "lang": v[1]["lang"],
                    "cat_0_name": v[1]["cat_0_name"],
                    "cat_1_name": v[1]["cat_1_name"],
                    "cat_2_name": v[1]["cat_2_name"],
                    "cat_3_name": v[1]["cat_3_name"],
                    "catalogname": v[1]["catalogname"],
                    "description": v[1]["description"],
                    "properties": v[1]["properties"],
                    "price": v[1]["price"],
                    "price_before_discount": v[1]["price_before_discount"],
                    "externallink": v[1]["externallink"],
                    "Rating": v[1]["Rating"],
                    "delivery": v[1]["delivery"],
                    "discount": v[1]["discount"],
                    "instock": v[1]["instock"],
                },
            }
            # logger.debug(payload)
            directory = get_directory(payload["row"]["LabebStoreId"])
            catalouge_v0 = v[0]["catalog_uuid"]
            catalouge_v1 = v[1]["catalog_uuid"]
            imgs_0 = encoded_images(directory, catalouge_v0)
            imgs_1 = encoded_images(directory, catalouge_v1)

            payload["row"]["images"] = imgs_0
            payload["nextRow"]["images"] = imgs_1
            try:
                print(f"""row: {len(payload["row"]["images"])}""")
                print(f"""nextRow: {len(payload["nextRow"]["images"])}""")
            except Exception as e:
                logger.error(e)
            # print(f"""row: {len(payload["row"]["images"])}""")
            # print(f"""nextRow: {len(payload["nextRow"]["images"])}""")
            response = client.post(
                f"""http://crawlerapi.labeb.com/api/PCCrawler/Crawl?StoreId={v[0]["LabebStoreId"]}""",
                json=payload,
            )
            logger.debug(f"Posting to {response.url}")
            logger.debug(f"""row: {payload["row"]["externallink"]}""")
            logger.debug(f"""nextRow: {payload["nextRow"]["externallink"]}""")
            logger.info(response.text)

        logger.info("-" * 80)


def post_main():
    try:
        with httpx.Client(headers=headers, timeout=300) as client:
            logging.info(f"---starting new---")
            files = glob("*.csv")
            for file in files:
                post_func(client, file)
            logging.info(f"---finished---")
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    post_main()
