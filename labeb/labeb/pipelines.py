# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.http import Request
from scrapy import signals
from urllib.parse import unquote
import os
import logging
from scrapy.exporters import CsvItemExporter
import pandas as pd
import csv
from scrapy.pipelines.images import ImagesPipeline
from scrapy.utils.project import get_project_settings


def dedup_csv_header(fname, fname_new):
    if not os.path.exists(fname):
        print("csv file not exist:", fname)
        return

    print(f"dedup csv headers, file {fname} to {fname_new}")
    fnew = open(fname_new, "w", encoding="utf-8")

    with open(fname, "r", encoding="utf-8") as f:
        header = None
        first = True
        for line in f:
            if None == header:
                header = line

            if not first and header == line:
                continue
            fnew.write(line)
            first = False

    fnew.close()


class LabebCsvPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        self.file = open("raw_labeb_items.csv", "ab")
        self.exporter = CsvItemExporter(self.file, encoding="utf-8-sig")
        self.exporter.fields_to_export = [
            "LabebStoreId",
            "catalog_uuid",
            "lang",
            "cat_0_name",
            "cat_1_name",
            "cat_2_name",
            "cat_3_name",
            "catalogname",
            "description",
            "properties",
            "price",
            "price_before_discount",
            "externallink",
            "Rating",
            "delivery",
            "discount",
            "instock",
        ]
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

        dedup_csv_header("raw_labeb_items.csv", "dev_labeb_items.csv")
        try:
            df = pd.read_csv("dev_labeb_items.csv")
            sort_df = df.sort_values(by=["catalog_uuid"])
            pd.concat(
                [
                    store.groupby("catalog_uuid", as_index=False)
                    .agg(list)
                    .reset_index(drop=True)
                    for _id, store in sort_df.groupby("LabebStoreId")
                ]
            ).sort_index().reset_index(drop=True).apply(pd.Series.explode).to_csv(
                "final_labeb_items.csv", index=False
            )

            with open(
                "final_labeb_items.csv", "r", encoding="utf-8-sig"
            ) as inputfile, open(
                "output.csv", "w", newline="", encoding="utf-8-sig"
            ) as outputfile:
                csv_in = csv.reader(inputfile)
                csv_out = csv.writer(outputfile)
                title = next(csv_in)
                csv_out.writerow(title)
                for row in csv_in:
                    if row != title:
                        csv_out.writerow(row)
        except pd.errors.EmptyDataError:
            spider.log("CSV pipeline No columns to parse from file", logging.ERROR)


class CarrefourKsaImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def file_path(self, request, response=None, info=None, *, item=None):
        file_name = os.path.basename(unquote(request.url))
        img_name = ""
        if ".jpg" in file_name:
            split_name = file_name.index(".jpg")
            img_name = file_name[:split_name] + ".jpg"
        if ".jpg" not in file_name:
            img_name = file_name + ".jpg"
        return item["path"] + img_name


class CarrefourKsaExcelPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        export_file = open(
            "%s_items_excel.csv" % spider.name,
            "ab" if os.path.isfile("%s-items.csv" % spider.name) else "wb",
        )
        self.file = export_file
        self.files[spider] = self.file
        self.exporter = CsvItemExporter(self.file)
        self.exporter.encoding = "utf-8"
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        dedup_csv_header(
            "%s_items_excel.csv" % spider.name,
            "%s-items-dropped-headers.csv" % spider.name,
        )
        try:
            df = pd.read_csv("%s-items-dropped-headers.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            sorted_df = drop_cols.sort_values(by=["catalog_uuid"])
            output_path = "%s-items-final.xlsx" % spider.name
            if not os.path.isfile(output_path):
                with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            else:
                with pd.ExcelWriter(
                    output_path,
                    mode="a",
                    engine="xlsxwriter",
                    if_sheet_exists="replace",
                ) as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            drop_dup = pd.read_excel(output_path)
            final_data = drop_dup.drop_duplicates()
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as pd_writer:
                final_data.to_excel(
                    pd_writer, sheet_name="Sheet1", header=True, index=False
                )

            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Excel pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)


class CarrefourUaeImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def file_path(self, request, response=None, info=None, *, item=None):
        file_name = os.path.basename(unquote(request.url))
        img_name = ""
        if ".jpg" in file_name:
            split_name = file_name.index(".jpg")
            img_name = file_name[:split_name] + ".jpg"
        if ".jpg" not in file_name:
            img_name = file_name + ".jpg"
        return item["path"] + img_name


class CarrefourUaeExcelPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        export_file = open(
            "%s_items_excel.csv" % spider.name,
            "ab" if os.path.isfile("%s-items.csv" % spider.name) else "wb",
        )
        self.file = export_file
        self.files[spider] = self.file
        self.exporter = CsvItemExporter(self.file)
        self.exporter.encoding = "utf-8"
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        dedup_csv_header(
            "%s_items_excel.csv" % spider.name,
            "%s-items-dropped-headers.csv" % spider.name,
        )
        try:
            df = pd.read_csv("%s-items-dropped-headers.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            sorted_df = drop_cols.sort_values(by=["catalog_uuid"])
            output_path = "%s-items-final.xlsx" % spider.name
            if not os.path.isfile(output_path):
                with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            else:
                with pd.ExcelWriter(
                    output_path,
                    mode="a",
                    engine="xlsxwriter",
                    if_sheet_exists="replace",
                ) as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            drop_dup = pd.read_excel(output_path)
            final_data = drop_dup.drop_duplicates()
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as pd_writer:
                final_data.to_excel(
                    pd_writer, sheet_name="Sheet1", header=True, index=False
                )

            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Excel pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)


class CarrefourQatarImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def file_path(self, request, response=None, info=None, *, item=None):
        file_name = os.path.basename(unquote(request.url))
        img_name = ""
        if ".jpg" in file_name:
            split_name = file_name.index(".jpg")
            img_name = file_name[:split_name] + ".jpg"
        if ".jpg" not in file_name:
            img_name = file_name + ".jpg"
        return item["path"] + img_name


class CarrefourQatarExcelPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        export_file = open(
            "%s_items_excel.csv" % spider.name,
            "ab" if os.path.isfile("%s-items.csv" % spider.name) else "wb",
        )
        self.file = export_file
        self.files[spider] = self.file
        self.exporter = CsvItemExporter(self.file)
        self.exporter.encoding = "utf-8"
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        dedup_csv_header(
            "%s_items_excel.csv" % spider.name,
            "%s-items-dropped-headers.csv" % spider.name,
        )
        try:
            df = pd.read_csv("%s-items-dropped-headers.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            sorted_df = drop_cols.sort_values(by=["catalog_uuid"])
            output_path = "%s-items-final.xlsx" % spider.name
            if not os.path.isfile(output_path):
                with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            else:
                with pd.ExcelWriter(
                    output_path,
                    mode="a",
                    engine="xlsxwriter",
                    if_sheet_exists="replace",
                ) as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            drop_dup = pd.read_excel(output_path)
            final_data = drop_dup.drop_duplicates()
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as pd_writer:
                final_data.to_excel(
                    pd_writer, sheet_name="Sheet1", header=True, index=False
                )

            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Excel pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)


class CarrefourKuwaitImagePipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def file_path(self, request, response=None, info=None, *, item=None):
        file_name = os.path.basename(unquote(request.url))
        img_name = ""
        if ".jpg" in file_name:
            split_name = file_name.index(".jpg")
            img_name = file_name[:split_name] + ".jpg"
        if ".jpg" not in file_name:
            img_name = file_name + ".jpg"
        return item["path"] + img_name


class CarrefourKuwaitExcelPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        export_file = open(
            "%s_items_excel.csv" % spider.name,
            "ab" if os.path.isfile("%s-items.csv" % spider.name) else "wb",
        )
        self.file = export_file
        self.files[spider] = self.file
        self.exporter = CsvItemExporter(self.file)
        self.exporter.encoding = "utf-8"
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        dedup_csv_header(
            "%s_items_excel.csv" % spider.name,
            "%s-items-dropped-headers.csv" % spider.name,
        )
        try:
            df = pd.read_csv("%s-items-dropped-headers.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            sorted_df = drop_cols.sort_values(by=["catalog_uuid"])
            output_path = "%s-items-final.xlsx" % spider.name
            if not os.path.isfile(output_path):
                with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            else:
                with pd.ExcelWriter(
                    output_path,
                    mode="a",
                    engine="xlsxwriter",
                    if_sheet_exists="replace",
                ) as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            drop_dup = pd.read_excel(output_path)
            final_data = drop_dup.drop_duplicates()
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as pd_writer:
                final_data.to_excel(
                    pd_writer, sheet_name="Sheet1", header=True, index=False
                )

            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Excel pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)


class CarrefourJordanImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def file_path(self, request, response=None, info=None, *, item=None):
        file_name = os.path.basename(unquote(request.url))
        img_name = ""
        if ".jpg" in file_name:
            split_name = file_name.index(".jpg")
            img_name = file_name[:split_name] + ".jpg"
        if ".jpg" not in file_name:
            img_name = file_name + ".jpg"
        return item["path"] + img_name


class CarrefourJordanExcelPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        export_file = open(
            "%s_items_excel.csv" % spider.name,
            "ab" if os.path.isfile("%s-items.csv" % spider.name) else "wb",
        )
        self.file = export_file
        self.files[spider] = self.file
        self.exporter = CsvItemExporter(self.file)
        self.exporter.encoding = "utf-8"
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        dedup_csv_header(
            "%s_items_excel.csv" % spider.name,
            "%s-items-dropped-headers.csv" % spider.name,
        )
        try:
            df = pd.read_csv("%s-items-dropped-headers.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            sorted_df = drop_cols.sort_values(by=["catalog_uuid"])
            output_path = "%s-items-final.xlsx" % spider.name
            if not os.path.isfile(output_path):
                with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            else:
                with pd.ExcelWriter(
                    output_path,
                    mode="a",
                    engine="xlsxwriter",
                    if_sheet_exists="replace",
                ) as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            drop_dup = pd.read_excel(output_path)
            final_data = drop_dup.drop_duplicates()
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as pd_writer:
                final_data.to_excel(
                    pd_writer, sheet_name="Sheet1", header=True, index=False
                )

            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Excel pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)


class LuluUaeImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def file_path(self, request, response=None, info=None, *, item=None):
        file_name = os.path.basename(unquote(request.url))
        img_name = ""
        if ".jpg" in file_name:
            split_name = file_name.index(".jpg")
            img_name = file_name[:split_name] + ".jpg"
        if ".jpg" not in file_name:
            img_name = file_name + ".jpg"
        return item["path"] + img_name


class LuluUaeExcelPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        export_file = open(
            "%s_items_excel.csv" % spider.name,
            "ab" if os.path.isfile("%s-items.csv" % spider.name) else "wb",
        )
        self.file = export_file
        self.files[spider] = self.file
        self.exporter = CsvItemExporter(self.file)
        self.exporter.encoding = "utf-8"
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        dedup_csv_header(
            "%s_items_excel.csv" % spider.name,
            "%s-items-dropped-headers.csv" % spider.name,
        )
        try:
            df = pd.read_csv("%s-items-dropped-headers.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            sorted_df = drop_cols.sort_values(by=["catalog_uuid"])
            output_path = "%s-items-final.xlsx" % spider.name
            if not os.path.isfile(output_path):
                with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            else:
                with pd.ExcelWriter(
                    output_path,
                    mode="a",
                    engine="xlsxwriter",
                    if_sheet_exists="replace",
                ) as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            drop_dup = pd.read_excel(output_path)
            final_data = drop_dup.drop_duplicates()
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as pd_writer:
                final_data.to_excel(
                    pd_writer, sheet_name="Sheet1", header=True, index=False
                )

            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Excel pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)


class LuluKsaImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def file_path(self, request, response=None, info=None, *, item=None):
        file_name = os.path.basename(unquote(request.url))
        img_name = ""
        if ".jpg" in file_name:
            split_name = file_name.index(".jpg")
            img_name = file_name[:split_name] + ".jpg"
        if ".jpg" not in file_name:
            img_name = file_name + ".jpg"
        return item["path"] + img_name


class LuluKsaExcelPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        export_file = open(
            "%s_items_excel.csv" % spider.name,
            "ab" if os.path.isfile("%s-items.csv" % spider.name) else "wb",
        )
        self.file = export_file
        self.files[spider] = self.file
        self.exporter = CsvItemExporter(self.file)
        self.exporter.encoding = "utf-8"
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        dedup_csv_header(
            "%s_items_excel.csv" % spider.name,
            "%s-items-dropped-headers.csv" % spider.name,
        )
        try:
            df = pd.read_csv("%s-items-dropped-headers.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            sorted_df = drop_cols.sort_values(by=["catalog_uuid"])
            output_path = "%s-items-final.xlsx" % spider.name
            if not os.path.isfile(output_path):
                with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            else:
                with pd.ExcelWriter(
                    output_path,
                    mode="a",
                    engine="xlsxwriter",
                    if_sheet_exists="replace",
                ) as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            drop_dup = pd.read_excel(output_path)
            final_data = drop_dup.drop_duplicates()
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as pd_writer:
                final_data.to_excel(
                    pd_writer, sheet_name="Sheet1", header=True, index=False
                )

            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Excel pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)


class LuluKuwaitImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def file_path(self, request, response=None, info=None, *, item=None):
        file_name = os.path.basename(unquote(request.url))
        img_name = ""
        if ".jpg" in file_name:
            split_name = file_name.index(".jpg")
            img_name = file_name[:split_name] + ".jpg"
        if ".jpg" not in file_name:
            img_name = file_name + ".jpg"
        return item["path"] + img_name


class LuluKuwaitExcelPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        export_file = open(
            "%s_items_excel.csv" % spider.name,
            "ab" if os.path.isfile("%s-items.csv" % spider.name) else "wb",
        )
        self.file = export_file
        self.files[spider] = self.file
        self.exporter = CsvItemExporter(self.file)
        self.exporter.encoding = "utf-8"
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        dedup_csv_header(
            "%s_items_excel.csv" % spider.name,
            "%s-items-dropped-headers.csv" % spider.name,
        )
        try:
            df = pd.read_csv("%s-items-dropped-headers.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            sorted_df = drop_cols.sort_values(by=["catalog_uuid"])
            output_path = "%s-items-final.xlsx" % spider.name
            if not os.path.isfile(output_path):
                with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            else:
                with pd.ExcelWriter(
                    output_path,
                    mode="a",
                    engine="xlsxwriter",
                    if_sheet_exists="replace",
                ) as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            drop_dup = pd.read_excel(output_path)
            final_data = drop_dup.drop_duplicates()
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as pd_writer:
                final_data.to_excel(
                    pd_writer, sheet_name="Sheet1", header=True, index=False
                )

            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Excel pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)


class LuluOmanImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def file_path(self, request, response=None, info=None, *, item=None):
        file_name = os.path.basename(unquote(request.url))
        img_name = ""
        if ".jpg" in file_name:
            split_name = file_name.index(".jpg")
            img_name = file_name[:split_name] + ".jpg"
        if ".jpg" not in file_name:
            img_name = file_name + ".jpg"
        return item["path"] + img_name


class LuluOmanExcelPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        export_file = open(
            "%s_items_excel.csv" % spider.name,
            "ab" if os.path.isfile("%s-items.csv" % spider.name) else "wb",
        )
        self.file = export_file
        self.files[spider] = self.file
        self.exporter = CsvItemExporter(self.file)
        self.exporter.encoding = "utf-8"
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        dedup_csv_header(
            "%s_items_excel.csv" % spider.name,
            "%s-items-dropped-headers.csv" % spider.name,
        )
        try:
            df = pd.read_csv("%s-items-dropped-headers.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            sorted_df = drop_cols.sort_values(by=["catalog_uuid"])
            output_path = "%s-items-final.xlsx" % spider.name
            if not os.path.isfile(output_path):
                with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            else:
                with pd.ExcelWriter(
                    output_path,
                    mode="a",
                    engine="xlsxwriter",
                    if_sheet_exists="replace",
                ) as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            drop_dup = pd.read_excel(output_path)
            final_data = drop_dup.drop_duplicates()
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as pd_writer:
                final_data.to_excel(
                    pd_writer, sheet_name="Sheet1", header=True, index=False
                )

            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Excel pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)


class LuluQatarImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def file_path(self, request, response=None, info=None, *, item=None):
        file_name = os.path.basename(unquote(request.url))
        img_name = ""
        if ".jpg" in file_name:
            split_name = file_name.index(".jpg")
            img_name = file_name[:split_name] + ".jpg"
        if ".jpg" not in file_name:
            img_name = file_name + ".jpg"
        return item["path"] + img_name


class LuluQatarExcelPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        export_file = open(
            "%s_items_excel.csv" % spider.name,
            "ab" if os.path.isfile("%s-items.csv" % spider.name) else "wb",
        )
        self.file = export_file
        self.files[spider] = self.file
        self.exporter = CsvItemExporter(self.file)
        self.exporter.encoding = "utf-8"
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        dedup_csv_header(
            "%s_items_excel.csv" % spider.name,
            "%s-items-dropped-headers.csv" % spider.name,
        )
        try:
            df = pd.read_csv("%s-items-dropped-headers.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            sorted_df = drop_cols.sort_values(by=["catalog_uuid"])
            output_path = "%s-items-final.xlsx" % spider.name
            if not os.path.isfile(output_path):
                with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            else:
                with pd.ExcelWriter(
                    output_path,
                    mode="a",
                    engine="xlsxwriter",
                    if_sheet_exists="replace",
                ) as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            drop_dup = pd.read_excel(output_path)
            final_data = drop_dup.drop_duplicates()
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as pd_writer:
                final_data.to_excel(
                    pd_writer, sheet_name="Sheet1", header=True, index=False
                )

            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Excel pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)


class LuluBahrainImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def file_path(self, request, response=None, info=None, *, item=None):
        file_name = os.path.basename(unquote(request.url))
        img_name = ""
        if ".jpg" in file_name:
            split_name = file_name.index(".jpg")
            img_name = file_name[:split_name] + ".jpg"
        if ".jpg" not in file_name:
            img_name = file_name + ".jpg"
        return item["path"] + img_name


class LuluBahrainExcelPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        export_file = open(
            "%s_items_excel.csv" % spider.name,
            "ab" if os.path.isfile("%s-items.csv" % spider.name) else "wb",
        )
        self.file = export_file
        self.files[spider] = self.file
        self.exporter = CsvItemExporter(self.file)
        self.exporter.encoding = "utf-8"
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        dedup_csv_header(
            "%s_items_excel.csv" % spider.name,
            "%s-items-dropped-headers.csv" % spider.name,
        )
        try:
            df = pd.read_csv("%s-items-dropped-headers.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            sorted_df = drop_cols.sort_values(by=["catalog_uuid"])
            output_path = "%s-items-final.xlsx" % spider.name
            if not os.path.isfile(output_path):
                with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            else:
                with pd.ExcelWriter(
                    output_path,
                    mode="a",
                    engine="xlsxwriter",
                    if_sheet_exists="replace",
                ) as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            drop_dup = pd.read_excel(output_path)
            final_data = drop_dup.drop_duplicates()
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as pd_writer:
                final_data.to_excel(
                    pd_writer, sheet_name="Sheet1", header=True, index=False
                )

            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Excel pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)


class LuluEgyptImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def file_path(self, request, response=None, info=None, *, item=None):
        file_name = os.path.basename(unquote(request.url))
        img_name = ""
        if ".jpg" in file_name:
            split_name = file_name.index(".jpg")
            img_name = file_name[:split_name] + ".jpg"
        if ".jpg" not in file_name:
            img_name = file_name + ".jpg"
        return item["path"] + img_name


class LuluEgyptExcelPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        export_file = open(
            "%s_items_excel.csv" % spider.name,
            "ab" if os.path.isfile("%s-items.csv" % spider.name) else "wb",
        )
        self.file = export_file
        self.files[spider] = self.file
        self.exporter = CsvItemExporter(self.file)
        self.exporter.encoding = "utf-8"
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()
        dedup_csv_header(
            "%s_items_excel.csv" % spider.name,
            "%s-items-dropped-headers.csv" % spider.name,
        )
        try:
            df = pd.read_csv("%s-items-dropped-headers.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            sorted_df = drop_cols.sort_values(by=["catalog_uuid"])
            output_path = "%s-items-final.xlsx" % spider.name
            if not os.path.isfile(output_path):
                with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            else:
                with pd.ExcelWriter(
                    output_path,
                    mode="a",
                    engine="xlsxwriter",
                    if_sheet_exists="replace",
                ) as writer:
                    sorted_df.to_excel(
                        writer, sheet_name="Sheet1", header=True, index=False
                    )
            drop_dup = pd.read_excel(output_path)
            final_data = drop_dup.drop_duplicates()
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as pd_writer:
                final_data.to_excel(
                    pd_writer, sheet_name="Sheet1", header=True, index=False
                )

            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Excel pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s-items-dropped-headers.csv" % spider.name)
            os.remove("%s_items_excel.csv" % spider.name)
