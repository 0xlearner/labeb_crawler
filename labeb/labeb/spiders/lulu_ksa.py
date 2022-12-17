import scrapy
import json
from urllib.parse import urlencode, unquote


class LuluKsa(scrapy.Spider):
    name = "5667_lulu_ksa"

    custom_settings = {
        "IMAGES_STORE": f"{name}_images",
        "ITEM_PIPELINES": {
            "labeb.pipelines.LuluKsaImagesPipeline": 1,
            "labeb.pipelines.LuluKsaCsvPipeline": 300,
            "labeb.pipelines.LuluKsaExcelPipeline": 345,
        },
    }

    cookies = {
        "XSRF-TOKEN": "4dead83a-52d6-4291-b2a8-100c580b99e3",
        "lulu-sa-theme": "alpha",
        "_gcl_au": "1.1.1893635688.1665495772",
        "_scid": "ac753939-a9f7-4761-895e-a8d8ff2fb9bb",
        "_fbp": "fb.1.1665495777255.203063257",
        "_sctr": "1|1665428400000",
        "_pxvid": "978c76cb-496a-11ed-b9c0-534655735978",
        "_fw_crm_v": "f58bd0e0-b84d-4e48-f1d8-b5febaab5b5b",
        "cookie-notification": "ACCEPTED",
        "anonymous-consents": "%5B%7B%22templateCode%22%3A%22PROFILE%22%2C%22templateVersion%22%3A1%2C%22consentState%22%3A%22GIVEN%22%7D%5D",
        "profile.consent.given": "true",
        "lulu-ae-theme": "alpha",
        "_gid": "GA1.2.888100566.1665690074",
        "lulu-my-theme": "alpha",
        "_ga_XSVL79H15S": "GS1.1.1665695895.1.1.1665695964.60.0.0",
        "lulu-sa-cart": "e4cbf157-718c-4d73-9a85-7720d4cc9221",
        "lulu-ae-cart": "e4cbf157-718c-4d73-9a85-7720d4cc9221",
        "_ga_DKGNXCKSFD": "GS1.1.1665761822.7.1.1665762532.54.0.0",
        "JSESSIONID": "Y14-8120af98-62ec-4693-928a-561fe13cad67.accstorefront-fcb686795-sxpkz",
        "ROUTE": ".accstorefront-fcb686795-sxpkz",
        "_pxhd": "7f96dc88658c5accc7237689dcf3b299e277346a87839d57f446af332146734b:978c76cb-496a-11ed-b9c0-534655735978",
        "pxcts": "1784b040-4e1e-11ed-8aa4-7968624c4a41",
        "_ga_4QT54T73J5": "GS1.1.1666012666.18.1.1666012827.21.0.0",
        "_ga_XK00CLKQ26": "GS1.1.1666012666.21.1.1666012828.1.0.0",
        "_ga_L6CBXGTM18": "GS1.1.1666012666.21.1.1666012828.0.0.0",
        "_ga": "GA1.2.520673390.1665495775",
        "_pxff_cc": "U2FtZVNpdGU9TGF4Ow==",
        "_px3": "a1235c08a7b860402af3714254a43a76b8e52a143202f340babf642d1533e282:2etGLrQL/MoMHNxtF4brH4xmWRM2u42XVxf9vFANoajBWgjGYlHaIQUMyBBlk/C8yp/VzAv4ffhvs9nH4xKeJQ==:1000:BgpQlbQpazexkorhCjzkvdWMV/YcQ7BG0H1jlwm0TpNpJZVb1H0ZzPuo0u18JazPYdL4KJ4RFLXtM/hwBdm7mEHWc1Qtu1HxhaA7L7OApbOKlnnUQ+XzA5NgOR901sCvVlCW+XjqTEQ0OD+zjda0VgEamKP23531vH+2qatDUsbg/aavVjRU4pz0UsTtr0Z1b2ZIwNo2SdHNte+K3gIHyg==",
    }

    headers = {
        "authority": "www.luluhypermarket.com",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-language": "en,ru;q=0.9",
        "cache-control": "max-age=0",
        "sec-ch-ua": '"Chromium";v="104", " Not A;Brand";v="99", "Yandex";v="22"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.114 YaBrowser/22.9.1.1110 (beta) Yowser/2.5 Safari/537.36",
    }

    def start_requests(self):
        categories = [
            "HY00215076",
            "HY00214990",
            "HY00214988",
            "HY00214796",
            "HY00214812",
            "HY00214804",
            "HY00214754",
            "HY00214762",
            "HY00215111",
            "HY00214730",
            "HY00214839",
            "HY00215333",
            "HY00214731",
            "HY00214728",
            "HY00214740",
            "HY00214852",
            "HY00214834",
            "HY00214832",
            "HY00216080",
            "HY00217325",
            "HY00214884",
            "HY00214870",
            "HY00215162",
            "HY00215180",
            "HY00216077",
            "HY00215200",
            "HY00217005",
            "HY002148010",
            "HY00217009",
            "HY00216375",
        ]

        for category in categories:
            base_url = f"https://www.luluhypermarket.com/en-sa/c/{category}/results-display?q=%3Adiscount-desc&sort=&page=0"
            yield scrapy.Request(
                url=base_url,
                headers=self.headers,
                cookies=self.cookies,
                dont_filter=True,
                callback=self.parse_links,
            )

    def parse_links(self, response):
        en_links = [
            link for link in response.css("a.js-gtm-product-link::attr(href)").extract()
        ]
        ar_links = [link.replace("/en-sa/", "/ar-sa/") for link in en_links]
        en_links.extend(ar_links)
        links = [response.urljoin(link) for link in en_links]
        total_pages = response.css(
            'input[id="plpNumberOfPagesCount"]::attr(value)'
        ).get()

        for link in links:
            yield scrapy.Request(
                url=link,
                headers=self.headers,
                cookies=self.cookies,
                dont_filter=True,
                callback=self.parse_product,
                meta={"product_url": link},
            )

    def parse_product(self, response):
        item = {}
        prod_url = response.meta.get("product_url")
        item["LabebStoreId"] = "5667"
        item["catalog_uuid"] = prod_url.split("/")[-1]
        item["lang"] = ""

        if "/en" in prod_url:
            item["lang"] = "en"
        if "/ar" in prod_url:
            item["lang"] = "ar"
        breadcrumbs = response.css("li.breadcrumb-item > a::text").extract()
        try:
            item["cat_0_name"] = breadcrumbs[0]
        except:
            item["cat_0_name"] = ""
        try:
            item["cat_1_name"] = breadcrumbs[1]
        except:
            item["cat_1_name"] = ""
        try:
            item["cat_2_name"] = breadcrumbs[2]
        except:
            item["cat_2_name"] = ""
        try:
            item["cat_3_name"] = breadcrumbs[3]
        except:
            item["cat_3_name"] = ""

        item["catalogname"] = response.css("h1.product-name::text").get().strip()
        try:
            description = [
                desc.replace("\n", "")
                for desc in response.css("div.col-md-12::text").extract()
            ]
            item["description"] = " ".join(list(filter(None, description)))
            if item["description"] == "   ":
                try:
                    item["description"] = " ".join(
                        response.css("div.col-md-12 > p ::text").getall()
                    )
                except:
                    item["description"] = ""
        except:
            item["description"] = ""
        raw_images = (
            response.css("div#productShowcaseCarousel")
            .css("span.zoom")
            .css("img::attr(src)")
            .extract()
        )

        clean_image_url = [response.urljoin(img_url) for img_url in raw_images]
        item["image_urls"] = clean_image_url
        try:
            keys = response.css("div.item").css("span.label::text").extract()
            values = response.css("div.item").css("span.value::text").extract()
            properties = {
                keys[i]: values[i].replace("\n", "") for i in range(len(keys))
            }
            raw_properties = json.dumps(properties, ensure_ascii=False).encode("utf-8")
            item["properties"] = raw_properties.decode()
        except:
            item["properties"] = ""
        item["price"] = response.css("span.item.price ::text").extract()[1]
        item["price_before_discount"] = response.css("span.off::text").get()
        item["externallink"] = prod_url
        item["path"] = f'catalouge_{item["catalog_uuid"]}/'
        item["Rating"] = (
            response.css('label[for="allReviews"] > span::text')
            .get()
            .replace("(", "")
            .replace(")", "")
        )
        delivery_yellow = response.css("span.icon-holder.yellow").get()
        delivery_green = response.css("span.icon-holder.green").get()
        delivery_red = response.css("span.icon-holder.red").get()
        dl_yellow = [
            dl.replace("\n", "")
            for dl in response.xpath(
                '//span[@class="icon-holder yellow"]/following-sibling::div//text()'
            ).getall()
        ]
        dl_green = [
            dl.replace("\n", "")
            for dl in response.xpath(
                '//span[@class="icon-holder green "]/following-sibling::div//text()'
            ).getall()
        ]
        dl_red = [
            dl.replace("\n", "")
            for dl in response.xpath(
                '//span[@class="icon-holder red "]/following-sibling::div//text()'
            ).getall()
        ]
        if delivery_yellow and delivery_green and delivery_red:
            yellow = " ".join(list(filter(None, dl_yellow)))
            green = " ".join(list(filter(None, dl_green)))
            red = " ".join(list(filter(None, dl_red)))
            item["delivery"] = yellow + "," + green + "," + red
        elif delivery_green and delivery_red:
            green = " ".join(list(filter(None, dl_green)))
            red = " ".join(list(filter(None, dl_red)))
            item["delivery"] = green + "," + red
        elif delivery_yellow:
            item["delivery"] = " ".join(list(filter(None, dl_yellow)))
        elif delivery_green:
            item["delivery"] = " ".join(list(filter(None, dl_green)))
        elif delivery_red:
            item["delivery"] = " ".join(list(filter(None, dl_red)))
        else:
            item["delivery"] = ""

        try:
            item["discount"] = response.css("span.item.off-percent::text").get()
        except:
            item["discount"] = ""
        stock = response.css("span.in-stock::text").get().strip().lower()
        if stock == "in stock" or stock == "متوفر":
            item["instock"] = "labeb"
        else:
            item["instock"] = ""

        yield item
