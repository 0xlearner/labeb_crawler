import scrapy
import json
from urllib.parse import urlencode, unquote
import base64
import requests


class CarrefourKSA(scrapy.Spider):
    name = "5638_carrefour_ksa"

    custom_settings = {
        "LOG_FILE": f"{name}.log",
        "IMAGES_STORE": "carrefour-ksa-images",
        "ITEM_PIPELINES": {
            "labeb.pipelines.CarrefourKsaImagesPipeline": 1,
            "labeb.pipelines.CarrefourKsaCsvPipeline": 300,
            "labeb.pipelines.CarrefourKsaExcelPipeline": 345,
        },
    }

    headers = {
        "authority": "www.carrefourksa.com",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-language": "en,ru;q=0.9",
        "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="100", "Yandex";v="22"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.143 YaBrowser/22.5.0.1879 (beta) Yowser/2.5 Safari/537.36",
    }

    def start_requests(self):
        categories = [
            "NFKSA4000000",
            "NFKSA1200000",
            "NFKSA2000000",
            "NFKSA2300000",
            "FKSA1000000",
            "NFKSA3000000",
            "NFKSA5000000",
            "NFKSA8000000",
            "NFKSA7000000",
            "NFKSA1400000",
        ]

        for category in categories:
            base_url = f"https://www.carrefourksa.com/mafsau/en/c/{category}?currentPage=0&filter=&nextPageOffset=0&pageSize=60&sortBy=relevance"
            yield scrapy.Request(
                url=base_url,
                headers=self.headers,
                callback=self.parse_links,
            )

    def parse_links(self, response):
        data = (
            response.css('script[id="__NEXT_DATA__"]')
            .get()
            .replace('<script id="__NEXT_DATA__" type="application/json">', "")
            .replace("</script>", "")
        )
        json_data = json.loads(data)

        product_listings = json_data["props"]["initialState"]["search"]["products"]
        en_listing_urls = [p["url"] for p in product_listings]
        ar_listing_urls = [link.replace("/en/", "/ar/") for link in en_listing_urls]
        en_listing_urls.extend(ar_listing_urls)
        total_pages = json_data["props"]["initialState"]["search"]["numOfPages"]

        # for i in range(1, 2):
        #     # next_url = (
        #     #     unquote(response.url).split("&")[1].replace("url=", "").split("?")[0]
        #     #     + f"?currentPage={i}&filter=&nextPageOffset=0&pageSize=60&sortBy=relevance"
        #     # )
        #     next_url = (
        #         unquote(response.url).split("?")[0]
        #         + f"?currentPage={i}&filter=&nextPageOffset=0&pageSize=60&sortBy=relevance"
        #     )
        #     yield scrapy.Request(
        #         url=next_url,
        #         headers=self.headers,
        #         callback=self.parse_links,
        #     )
        for product_link in en_listing_urls:
            product_url = "https://www.carrefourksa.com/" + product_link

            yield scrapy.Request(
                url=product_url,
                headers=self.headers,
                callback=self.parse_product,
            )

    def parse_product(self, response):
        item = {}
        data = (
            response.css('script[id="__NEXT_DATA__"]')
            .get()
            .replace('<script id="__NEXT_DATA__" type="application/json">', "")
            .replace("</script>", "")
        )
        json_data = json.loads(data)
        link_url = unquote(response.url)
        item["LabebStoreId"] = "5638"
        item["catalog_uuid"] = ""

        item["lang"] = ""
        if "/en/" in link_url:
            item["lang"] = "en"
        if "/ar/" in link_url:
            item["lang"] = "ar"
        breadcrumb = response.css("div.css-iamwo8 > a::text").extract()[1:]
        item["cat_0_name"] = ""
        item["cat_1_name"] = ""
        item["cat_2_name"] = ""
        item["cat_3_name"] = ""
        try:
            item["cat_0_name"] = breadcrumb[0]
        except:
            item["cat_0_name"] = ""
        try:
            item["cat_1_name"] = breadcrumb[1]
        except:
            item["cat_1_name"] = ""
        try:
            item["cat_2_name"] = breadcrumb[2]
        except:
            item["cat_2_name"] = ""
        try:
            item["cat_3_name"] = breadcrumb[3]
        except:
            item["cat_3_name"] = ""
        item["catalogname"] = response.css("h1.css-106scfp::text").get()
        item_attr = json_data["props"]["initialProps"]["pageProps"]["initialData"][
            "products"
        ][0]["attributes"]
        item["description"] = ""
        try:
            item["description"] = ", ".join(
                response.css("div.css-16lm0vc ::text").getall()
            )
        except:
            item["description"] = ""
        if item["description"] == "":
            try:
                item["description"] = (
                    item_attr["marketingText"]
                    .replace("<ul>", "")
                    .replace("<li>", "")
                    .replace("</li>", "")
                    .replace("</ul>", "")
                    .strip()
                )
            except:
                item["description"] = ""
        raw_images = response.css("div.css-1c2pck7 ::attr(src)").getall()
        raw_encoded_images = []
        for img in raw_images:
            resp = requests.get(img, headers=self.headers)
            img_uri = (
                "data:"
                + resp.headers["Content-Type"]
                + ";"
                + "base64,"
                + base64.b64encode(resp.content).decode("utf-8")
            )
            raw_encoded_images.append(
                img_uri.replace("data:image/webp;base64", "data:image/jpeg;base64")
            )

        clean_image_url = []

        for img_url in raw_images:
            clean_image_url.append(response.urljoin(img_url))

        item["image_urls"] = clean_image_url
        item["properties"] = "{}"
        try:
            json_features_source = json_data["props"]["initialProps"]["pageProps"][
                "initialData"
            ]["products"][0]
            features_list = [
                f["features"] for f in json_features_source["classificationAttributes"]
            ]
            features_new = {}
            for x in features_list:
                for y in x:
                    for k, v in y.items():
                        features_new[k] = v.replace(
                            "<\x00i\x00g\x00n\x00o\x00r\x00e\x00>", ""
                        )
                        break
            if "ingredients" in json_features_source["attributes"]:
                features_new["ingredients"] = json_features_source["attributes"][
                    "ingredients"
                ]
            if "safetyWarnings" in json_features_source["attributes"]:
                features_new["safetyWarnings"] = json_features_source["attributes"][
                    "safetyWarnings"
                ]
            if "storageConditions" in json_features_source["attributes"]:
                features_new["storageConditions"] = json_features_source["attributes"][
                    "storageConditions"
                ]
            raw_properties = json.dumps(features_new, ensure_ascii=False).encode(
                "utf-8"
            )
            item["properties"] = raw_properties.decode()
        except:
            item["properties"] = "{}"

        # keys = response.css("div.css-pi51ey::text").getall()
        # values = response.css("h3.css-1ps12pz::text").getall()
        # if keys and values:
        #     properties = {keys[i]: values[i] for i in range(len(keys))}
        #     raw_properties = json.dumps(properties, ensure_ascii=False).encode("utf-8")
        #     item["properties"] = raw_properties.decode()
        # else:
        #     try:
        #         features = json_data["props"]["initialProps"]["pageProps"][
        #             "initialData"
        #         ]["products"][0]["classificationAttributes"][0]["features"]
        #         features_dict = dict(next(iter(d.items())) for d in features)
        #         try:
        #             features_dict["ingredients"] = item_attr["ingredients"]
        #             features_dict["safetyWarnings"] = item_attr["safetyWarnings"]
        #             features_dict["storageConditions"] = item_attr["storageConditions"]
        #         except KeyError:
        #             pass
        #         raw_features = json.dumps(features_dict, ensure_ascii=False).encode(
        #             "utf-8"
        #         )
        #         item["properties"] = raw_features.decode()
        #     except:
        #         item["properties"] = "{}"
        try:
            item["price"] = response.css("h2.css-1i90gmp::text").getall()[2]
        except:
            item["price"] = response.css("h2.css-17ctnp::text").getall()[2]
        try:
            item["price_before_discount"] = response.css(
                "del.css-1bdwabt::text"
            ).getall()[2]
        except:
            item["price_before_discount"] = ""
        # item["externallink"] = link_url.split("=")[2]
        item["externallink"] = link_url
        item["catalog_uuid"] = item["externallink"].split("/")[-1]
        item["path"] = f'catalouge_{item["catalog_uuid"]}/'
        item["Rating"] = ""
        item["delivery"] = response.css("span.css-u98ylp::text").get()
        try:
            item[
                "discount"
            ] = f'{json_data["props"]["initialProps"]["pageProps"]["initialData"]["products"][0]["offers"][0]["stores"][0]["price"]["discount"]["information"]["amount"]}%'
        except:
            item["discount"] = ""
        try:
            item["instock"] = response.css("div.css-g4iap9::text").extract()[1]
        except:
            item["instock"] = ""
        item["encoded_images"] = raw_encoded_images
        yield item
