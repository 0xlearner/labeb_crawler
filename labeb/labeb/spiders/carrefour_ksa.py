import scrapy
import json
from urllib.parse import urlencode, unquote


class CarrefourKSA(scrapy.Spider):
    name = "5638_carrefour_ksa"

    custom_settings = {
        "IMAGES_STORE": f"{name}_images",
        "ITEM_PIPELINES": {
            "labeb.pipelines.CarrefourKsaImagesPipeline": 1,
            "labeb.pipelines.CarrefourKsaCsvPipeline": 300,
            "labeb.pipelines.CarrefourKsaExcelPipeline": 345,
        },
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        # 'Accept-Encoding': 'gzip, deflate, br',
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "Sec-GPC": "1",
        "Connection": "keep-alive",
        # 'Cookie': 'cart_api=v2; app_version=v4; page_type=categorySearch; cart_api=v2; cart_api=v2; _abck=0435527AA58124F09B72E596F8544495~-1~YAAQRaMAF/4PYBaFAQAAavG6HgkU801N10JBy3UReNQSbUqUC0H8Ybdu74xNL6N8KLl9Xov2GYoBI6S04mcmxHVSIoRFqSF5tqt0I9CwgpWggZ+6xve1drIUyYkjmS1c62cyaGQf1i/FZVEENT/uthCJPreRiDSTIDrvIGFiW0l8aoZqhGf9n6hu2LNEmzSOCtGj21eNH4pEbhiBDP5NOG5Q9AZwrNwCM/8oz7XFX4Nw1zrYzvXbpx9tVBfr+Gim8CeXxVjJcqdRT0qe4jpAhYUjYgwwNGwovR4tx9+/enX2tOCBVyaGPwQ5AdUeNWitgEY3axaHWWfAQjcjpO9tB1kZyibPM3j+YSBaG8C4HPyTVAucjfljiXnJ72aoxoLsniP47rHKhKjGeMN+sj7eeg==~-1~-1~-1; storeInfo=mafsau|en|SAR; maf-session-id=db8b2be0-15d3-4aef-9b70-13b01b193939; mafsau-preferred-delivery-area=Granada - Riyadh; prevAreaCode=Granada - Riyadh; ak_bmsc=39EC4BA2231740F64190B246CA1E09C9~000000000000000000000000000000~YAAQRaMAFzYQYBaFAQAAAfm6HhIweJNARyeyucdjgQZ2EMHOxAnGziZBH2wnzl6zQ0KtqXLdIxPq3DrJ5z2g5k8+uXLUBB27waEhjk9JA4JgYctUT74lK3jA5Xmo5KR8mG8f411GcgluaxkVxhoybfa52eV1Uo+NkLJPcmemFL1fdhNDhSwZZIW3MMBVeubHv14s//WaU7cAcn+gNTc3WI5P1zSyfWePqvC3WXbNwANZDZVvw4WRl8xglHPW+H8oX5hoNV6UX/eQTLQdE3ht6mva5XHTW8AxErCN3zEKXtASNPazTUzbqxOlZuqeai9xHvNEQc8r1BiXMWgUBGf+NDx/19GtsB195Ii3YseQ+IiAtQ0ZAX3THrtczWNynhTmpItD8IR8oCuToyL7hjyUAqfiHrpOXOfxcPvVPuMV1hQzbBtE8/5heTkMWYZs3gvJPgcQHhtOWIC0MTGjylCvIcDiC0jFGyqhdYiUJpcKwvePUyZaY48xNuJO4AWuP+/oAYPlXQtEzVI5lUv/b7WgKsq7Mtbp2Wx5IiscYAFoW5UIm0QUO7w=; bm_sz=C56BBFB2F3D957FE59818E6720AB9EFE~YAAQRaMAFwEQYBaFAQAAavG6HhJOwsuhNF0hTOFVmAmsbTQq7I6zeF2yd9WGwzUvBg3NaDndUagNh5oC3B91a26Mps47vuhwI69PaOcshoubdO2JP2uIG/y6T8ECVIYXqRdk+Hs/N+8AYL4UPm5fG71PKZ7WRWslN1qUu8Wbqg0C4ODncSwF78Hov8hC9mJECihKVwcU7RyIUED6YKWw8Cvf5+Wsw5ZUEZe7cPFoqJcbNVCzhuvOIE/9tIvXP5a4HqUKxR2PUN5+cA2FDDYxuzs0tEsWEpc+BmT4ofHfdub09N8qJrqcNaE=~3616836~4539442; AKA_A2=A',
        # Requests doesn't support trailers
        # 'TE': 'trailers',
        "If-None-Match": '"40aaa-Cd++C758T+m5S6RM0p6ux3Z3oCs"',
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

        yield item
