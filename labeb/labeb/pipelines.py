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


class CarrefourKsaCsvPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        # self.file = open("%s-items.csv" % spider.name, "ab", newline="")
        self.items = []
        self.colnames = []

    def process_item(self, item, spider):
        # add the new fields
        for f in item.keys():
            if f not in self.colnames:
                self.colnames.append(f)

        # add the item itself to the list
        self.items.append(item)
        return item

    def spider_closed(self, spider):
        items_csv = os.path.isfile("%s_items.csv" % spider.name)
        with open("%s_items.csv" % spider.name, "a", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.colnames)

            if not items_csv:
                writer.writeheader()

            for item in self.items:
                writer.writerow(item)
            with open(
                "%s_items.csv" % spider.name, "r", encoding="utf-8-sig"
            ) as duplicate_csv, open(
                "%s_items_raw.csv" % spider.name, "w", encoding="utf-8-sig"
            ) as out_file:
                seen = set()
                for line in duplicate_csv:
                    if line in seen:
                        continue

                    seen.add(line)
                    out_file.write(line)
        os.remove("%s_items.csv" % spider.name)
        try:
            df = pd.read_csv("%s_items_raw.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            drop_dup = drop_cols.drop_duplicates(subset=["externallink"])
            sorted_df = drop_dup.sort_values(by=["catalog_uuid"])
            output_path = "%s_items_final.csv" % spider.name
            if not os.path.isfile(output_path):
                sorted_df.to_csv("%s_items_final.csv" % spider.name, index=False)
            else:
                sorted_df.to_csv(
                    "%s_items_final.csv" % spider.name,
                    mode="a",
                    encoding="utf-8-sig",
                    header=False,
                    index=False,
                )
            dedup = pd.read_csv("%s_items_final.csv" % spider.name, skiprows=0)
            final_data = dedup.drop_duplicates()
            final_data.to_csv("%s_items_final.csv" % spider.name, index=False)

            os.remove("%s_items_raw.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Csv pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s_items_raw.csv" % spider.name)


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


class CarrefourUaeCsvPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        # self.file = open("%s-items.csv" % spider.name, "ab", newline="")
        self.items = []
        self.colnames = []

    def process_item(self, item, spider):
        # add the new fields
        for f in item.keys():
            if f not in self.colnames:
                self.colnames.append(f)

        # add the item itself to the list
        self.items.append(item)
        return item

    def spider_closed(self, spider):
        items_csv = os.path.isfile("%s_items.csv" % spider.name)
        with open("%s_items.csv" % spider.name, "a", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.colnames)

            if not items_csv:
                writer.writeheader()

            for item in self.items:
                writer.writerow(item)
            with open(
                "%s_items.csv" % spider.name, "r", encoding="utf-8-sig"
            ) as duplicate_csv, open(
                "%s_items_raw.csv" % spider.name, "w", encoding="utf-8-sig"
            ) as out_file:
                seen = set()
                for line in duplicate_csv:
                    if line in seen:
                        continue

                    seen.add(line)
                    out_file.write(line)
        os.remove("%s_items.csv" % spider.name)

        try:
            df = pd.read_csv("%s_items_raw.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            drop_dup = drop_cols.drop_duplicates(subset=["externallink"])
            sorted_df = drop_dup.sort_values(by=["catalog_uuid"])
            output_path = "%s_items_final.csv" % spider.name
            if not os.path.isfile(output_path):
                sorted_df.to_csv("%s_items_final.csv" % spider.name, index=False)
            else:
                sorted_df.to_csv(
                    "%s_items_final.csv" % spider.name,
                    mode="a",
                    encoding="utf-8-sig",
                    header=False,
                    index=False,
                )
            dedup = pd.read_csv("%s_items_final.csv" % spider.name, skiprows=0)
            final_data = dedup.drop_duplicates()
            final_data.to_csv("%s_items_final.csv" % spider.name, index=False)

            os.remove("%s_items_raw.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Csv pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s_items_raw.csv" % spider.name)


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


class CarrefourQatarCsvPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        # self.file = open("%s-items.csv" % spider.name, "ab", newline="")
        self.items = []
        self.colnames = []

    def process_item(self, item, spider):
        # add the new fields
        for f in item.keys():
            if f not in self.colnames:
                self.colnames.append(f)

        # add the item itself to the list
        self.items.append(item)
        return item

    def spider_closed(self, spider):
        items_csv = os.path.isfile("%s_items.csv" % spider.name)
        with open("%s_items.csv" % spider.name, "a", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.colnames)

            if not items_csv:
                writer.writeheader()

            for item in self.items:
                writer.writerow(item)
            with open(
                "%s_items.csv" % spider.name, "r", encoding="utf-8-sig"
            ) as duplicate_csv, open(
                "%s_items_raw.csv" % spider.name, "w", encoding="utf-8-sig"
            ) as out_file:
                seen = set()
                for line in duplicate_csv:
                    if line in seen:
                        continue

                    seen.add(line)
                    out_file.write(line)
        os.remove("%s_items.csv" % spider.name)

        try:
            df = pd.read_csv("%s_items_raw.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            drop_dup = drop_cols.drop_duplicates(subset=["externallink"])
            sorted_df = drop_dup.sort_values(by=["catalog_uuid"])
            output_path = "%s_items_final.csv" % spider.name
            if not os.path.isfile(output_path):
                sorted_df.to_csv("%s_items_final.csv" % spider.name, index=False)
            else:
                sorted_df.to_csv(
                    "%s_items_final.csv" % spider.name,
                    mode="a",
                    encoding="utf-8-sig",
                    header=False,
                    index=False,
                )
            dedup = pd.read_csv("%s_items_final.csv" % spider.name, skiprows=0)
            final_data = dedup.drop_duplicates()
            final_data.to_csv("%s_items_final.csv" % spider.name, index=False)

            os.remove("%s_items_raw.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Csv pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s_items_raw.csv" % spider.name)


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


class CarrefourKuwaitCsvPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        # self.file = open("%s-items.csv" % spider.name, "ab", newline="")
        self.items = []
        self.colnames = []

    def process_item(self, item, spider):
        # add the new fields
        for f in item.keys():
            if f not in self.colnames:
                self.colnames.append(f)

        # add the item itself to the list
        self.items.append(item)
        return item

    def spider_closed(self, spider):
        items_csv = os.path.isfile("%s_items.csv" % spider.name)
        with open("%s_items.csv" % spider.name, "a", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.colnames)

            if not items_csv:
                writer.writeheader()

            for item in self.items:
                writer.writerow(item)
            with open(
                "%s_items.csv" % spider.name, "r", encoding="utf-8-sig"
            ) as duplicate_csv, open(
                "%s_items_raw.csv" % spider.name, "w", encoding="utf-8-sig"
            ) as out_file:
                seen = set()
                for line in duplicate_csv:
                    if line in seen:
                        continue

                    seen.add(line)
                    out_file.write(line)
        os.remove("%s_items.csv" % spider.name)

        try:
            df = pd.read_csv("%s_items_raw.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            drop_dup = drop_cols.drop_duplicates(subset=["externallink"])
            sorted_df = drop_dup.sort_values(by=["catalog_uuid"])
            output_path = "%s_items_final.csv" % spider.name
            if not os.path.isfile(output_path):
                sorted_df.to_csv("%s_items_final.csv" % spider.name, index=False)
            else:
                sorted_df.to_csv(
                    "%s_items_final.csv" % spider.name,
                    mode="a",
                    encoding="utf-8-sig",
                    header=False,
                    index=False,
                )
            dedup = pd.read_csv("%s_items_final.csv" % spider.name, skiprows=0)
            final_data = dedup.drop_duplicates()
            final_data.to_csv("%s_items_final.csv" % spider.name, index=False)

            os.remove("%s_items_raw.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Csv pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s_items_raw.csv" % spider.name)


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


class CarrefourJordanCsvPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        # self.file = open("%s-items.csv" % spider.name, "ab", newline="")
        self.items = []
        self.colnames = []

    def process_item(self, item, spider):
        # add the new fields
        for f in item.keys():
            if f not in self.colnames:
                self.colnames.append(f)

        # add the item itself to the list
        self.items.append(item)
        return item

    def spider_closed(self, spider):
        items_csv = os.path.isfile("%s_items.csv" % spider.name)
        with open("%s_items.csv" % spider.name, "a", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.colnames)

            if not items_csv:
                writer.writeheader()

            for item in self.items:
                writer.writerow(item)
            with open(
                "%s_items.csv" % spider.name, "r", encoding="utf-8-sig"
            ) as duplicate_csv, open(
                "%s_items_raw.csv" % spider.name, "w", encoding="utf-8-sig"
            ) as out_file:
                seen = set()
                for line in duplicate_csv:
                    if line in seen:
                        continue

                    seen.add(line)
                    out_file.write(line)
        os.remove("%s_items.csv" % spider.name)

        try:
            df = pd.read_csv("%s_items_raw.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            drop_dup = drop_cols.drop_duplicates(subset=["externallink"])
            sorted_df = drop_dup.sort_values(by=["catalog_uuid"])
            output_path = "%s_items_final.csv" % spider.name
            if not os.path.isfile(output_path):
                sorted_df.to_csv("%s_items_final.csv" % spider.name, index=False)
            else:
                sorted_df.to_csv(
                    "%s_items_final.csv" % spider.name,
                    mode="a",
                    encoding="utf-8-sig",
                    header=False,
                    index=False,
                )
            dedup = pd.read_csv("%s_items_final.csv" % spider.name, skiprows=0)
            final_data = dedup.drop_duplicates()
            final_data.to_csv("%s_items_final.csv" % spider.name, index=False)

            os.remove("%s_items_raw.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Csv pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s_items_raw.csv" % spider.name)


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


class LuluUaeCsvPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        # self.file = open("%s-items.csv" % spider.name, "ab", newline="")
        self.items = []
        self.colnames = []

    def process_item(self, item, spider):
        # add the new fields
        for f in item.keys():
            if f not in self.colnames:
                self.colnames.append(f)

        # add the item itself to the list
        self.items.append(item)
        return item

    def spider_closed(self, spider):
        items_csv = os.path.isfile("%s_items.csv" % spider.name)
        with open("%s_items.csv" % spider.name, "a", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.colnames)

            if not items_csv:
                writer.writeheader()

            for item in self.items:
                writer.writerow(item)
            with open(
                "%s_items.csv" % spider.name, "r", encoding="utf-8-sig"
            ) as duplicate_csv, open(
                "%s_items_raw.csv" % spider.name, "w", encoding="utf-8-sig"
            ) as out_file:
                seen = set()
                for line in duplicate_csv:
                    if line in seen:
                        continue

                    seen.add(line)
                    out_file.write(line)
        os.remove("%s_items.csv" % spider.name)

        try:
            df = pd.read_csv("%s_items_raw.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            drop_dup = drop_cols.drop_duplicates(subset=["externallink"])
            sorted_df = drop_dup.sort_values(by=["catalog_uuid"])
            output_path = "%s_items_final.csv" % spider.name
            if not os.path.isfile(output_path):
                sorted_df.to_csv("%s_items_final.csv" % spider.name, index=False)
            else:
                sorted_df.to_csv(
                    "%s_items_final.csv" % spider.name,
                    mode="a",
                    encoding="utf-8-sig",
                    header=False,
                    index=False,
                )
            dedup = pd.read_csv("%s_items_final.csv" % spider.name, skiprows=0)
            final_data = dedup.drop_duplicates()
            final_data.to_csv("%s_items_final.csv" % spider.name, index=False)

            os.remove("%s_items_raw.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Csv pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s_items_raw.csv" % spider.name)


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


class LuluKsaCsvPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        # self.file = open("%s-items.csv" % spider.name, "ab", newline="")
        self.items = []
        self.colnames = []

    def process_item(self, item, spider):
        # add the new fields
        for f in item.keys():
            if f not in self.colnames:
                self.colnames.append(f)

        # add the item itself to the list
        self.items.append(item)
        return item

    def spider_closed(self, spider):
        items_csv = os.path.isfile("%s_items.csv" % spider.name)
        with open("%s_items.csv" % spider.name, "a", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.colnames)

            if not items_csv:
                writer.writeheader()

            for item in self.items:
                writer.writerow(item)
            with open(
                "%s_items.csv" % spider.name, "r", encoding="utf-8-sig"
            ) as duplicate_csv, open(
                "%s_items_raw.csv" % spider.name, "w", encoding="utf-8-sig"
            ) as out_file:
                seen = set()
                for line in duplicate_csv:
                    if line in seen:
                        continue

                    seen.add(line)
                    out_file.write(line)
        os.remove("%s_items.csv" % spider.name)

        try:
            df = pd.read_csv("%s_items_raw.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            drop_dup = drop_cols.drop_duplicates(subset=["externallink"])
            sorted_df = drop_dup.sort_values(by=["catalog_uuid"])
            output_path = "%s_items_final.csv" % spider.name
            if not os.path.isfile(output_path):
                sorted_df.to_csv("%s_items_final.csv" % spider.name, index=False)
            else:
                sorted_df.to_csv(
                    "%s_items_final.csv" % spider.name,
                    mode="a",
                    encoding="utf-8-sig",
                    header=False,
                    index=False,
                )
            dedup = pd.read_csv("%s_items_final.csv" % spider.name, skiprows=0)
            final_data = dedup.drop_duplicates()
            final_data.to_csv("%s_items_final.csv" % spider.name, index=False)

            os.remove("%s_items_raw.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Csv pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s_items_raw.csv" % spider.name)


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


class LuluKuwaitCsvPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        # self.file = open("%s-items.csv" % spider.name, "ab", newline="")
        self.items = []
        self.colnames = []

    def process_item(self, item, spider):
        # add the new fields
        for f in item.keys():
            if f not in self.colnames:
                self.colnames.append(f)

        # add the item itself to the list
        self.items.append(item)
        return item

    def spider_closed(self, spider):
        items_csv = os.path.isfile("%s_items.csv" % spider.name)
        with open("%s_items.csv" % spider.name, "a", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.colnames)

            if not items_csv:
                writer.writeheader()

            for item in self.items:
                writer.writerow(item)
            with open(
                "%s_items.csv" % spider.name, "r", encoding="utf-8-sig"
            ) as duplicate_csv, open(
                "%s_items_raw.csv" % spider.name, "w", encoding="utf-8-sig"
            ) as out_file:
                seen = set()
                for line in duplicate_csv:
                    if line in seen:
                        continue

                    seen.add(line)
                    out_file.write(line)
        os.remove("%s_items.csv" % spider.name)

        try:
            df = pd.read_csv("%s_items_raw.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            drop_dup = drop_cols.drop_duplicates(subset=["externallink"])
            sorted_df = drop_dup.sort_values(by=["catalog_uuid"])
            output_path = "%s_items_final.csv" % spider.name
            if not os.path.isfile(output_path):
                sorted_df.to_csv("%s_items_final.csv" % spider.name, index=False)
            else:
                sorted_df.to_csv(
                    "%s_items_final.csv" % spider.name,
                    mode="a",
                    encoding="utf-8-sig",
                    header=False,
                    index=False,
                )
            dedup = pd.read_csv("%s_items_final.csv" % spider.name, skiprows=0)
            final_data = dedup.drop_duplicates()
            final_data.to_csv("%s_items_final.csv" % spider.name, index=False)

            os.remove("%s_items_raw.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Csv pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s_items_raw.csv" % spider.name)


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


class LuluOmanCsvPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        # self.file = open("%s-items.csv" % spider.name, "ab", newline="")
        self.items = []
        self.colnames = []

    def process_item(self, item, spider):
        # add the new fields
        for f in item.keys():
            if f not in self.colnames:
                self.colnames.append(f)

        # add the item itself to the list
        self.items.append(item)
        return item

    def spider_closed(self, spider):
        items_csv = os.path.isfile("%s_items.csv" % spider.name)
        with open("%s_items.csv" % spider.name, "a", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.colnames)

            if not items_csv:
                writer.writeheader()

            for item in self.items:
                writer.writerow(item)
            with open(
                "%s_items.csv" % spider.name, "r", encoding="utf-8-sig"
            ) as duplicate_csv, open(
                "%s_items_raw.csv" % spider.name, "w", encoding="utf-8-sig"
            ) as out_file:
                seen = set()
                for line in duplicate_csv:
                    if line in seen:
                        continue

                    seen.add(line)
                    out_file.write(line)
        os.remove("%s_items.csv" % spider.name)

        try:
            df = pd.read_csv("%s_items_raw.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            drop_dup = drop_cols.drop_duplicates(subset=["externallink"])
            sorted_df = drop_dup.sort_values(by=["catalog_uuid"])
            output_path = "%s_items_final.csv" % spider.name
            if not os.path.isfile(output_path):
                sorted_df.to_csv("%s_items_final.csv" % spider.name, index=False)
            else:
                sorted_df.to_csv(
                    "%s_items_final.csv" % spider.name,
                    mode="a",
                    encoding="utf-8-sig",
                    header=False,
                    index=False,
                )
            dedup = pd.read_csv("%s_items_final.csv" % spider.name, skiprows=0)
            final_data = dedup.drop_duplicates()
            final_data.to_csv("%s_items_final.csv" % spider.name, index=False)

            os.remove("%s_items_raw.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Csv pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s_items_raw.csv" % spider.name)


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


class LuluQatarCsvPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        # self.file = open("%s-items.csv" % spider.name, "ab", newline="")
        self.items = []
        self.colnames = []

    def process_item(self, item, spider):
        # add the new fields
        for f in item.keys():
            if f not in self.colnames:
                self.colnames.append(f)

        # add the item itself to the list
        self.items.append(item)
        return item

    def spider_closed(self, spider):
        items_csv = os.path.isfile("%s_items.csv" % spider.name)
        with open("%s_items.csv" % spider.name, "a", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.colnames)

            if not items_csv:
                writer.writeheader()

            for item in self.items:
                writer.writerow(item)
            with open(
                "%s_items.csv" % spider.name, "r", encoding="utf-8-sig"
            ) as duplicate_csv, open(
                "%s_items_raw.csv" % spider.name, "w", encoding="utf-8-sig"
            ) as out_file:
                seen = set()
                for line in duplicate_csv:
                    if line in seen:
                        continue

                    seen.add(line)
                    out_file.write(line)
        os.remove("%s_items.csv" % spider.name)

        try:
            df = pd.read_csv("%s_items_raw.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            drop_dup = drop_cols.drop_duplicates(subset=["externallink"])
            sorted_df = drop_dup.sort_values(by=["catalog_uuid"])
            output_path = "%s_items_final.csv" % spider.name
            if not os.path.isfile(output_path):
                sorted_df.to_csv("%s_items_final.csv" % spider.name, index=False)
            else:
                sorted_df.to_csv(
                    "%s_items_final.csv" % spider.name,
                    mode="a",
                    encoding="utf-8-sig",
                    header=False,
                    index=False,
                )
            dedup = pd.read_csv("%s_items_final.csv" % spider.name, skiprows=0)
            final_data = dedup.drop_duplicates()
            final_data.to_csv("%s_items_final.csv" % spider.name, index=False)

            os.remove("%s_items_raw.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Csv pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s_items_raw.csv" % spider.name)


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


class LuluBahrainCsvPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        # self.file = open("%s-items.csv" % spider.name, "ab", newline="")
        self.items = []
        self.colnames = []

    def process_item(self, item, spider):
        # add the new fields
        for f in item.keys():
            if f not in self.colnames:
                self.colnames.append(f)

        # add the item itself to the list
        self.items.append(item)
        return item

    def spider_closed(self, spider):
        items_csv = os.path.isfile("%s_items.csv" % spider.name)
        with open("%s_items.csv" % spider.name, "a", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.colnames)

            if not items_csv:
                writer.writeheader()

            for item in self.items:
                writer.writerow(item)
            with open(
                "%s_items.csv" % spider.name, "r", encoding="utf-8-sig"
            ) as duplicate_csv, open(
                "%s_items_raw.csv" % spider.name, "w", encoding="utf-8-sig"
            ) as out_file:
                seen = set()
                for line in duplicate_csv:
                    if line in seen:
                        continue

                    seen.add(line)
                    out_file.write(line)
        os.remove("%s_items.csv" % spider.name)

        try:
            df = pd.read_csv("%s_items_raw.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            drop_dup = drop_cols.drop_duplicates(subset=["externallink"])
            sorted_df = drop_dup.sort_values(by=["catalog_uuid"])
            output_path = "%s_items_final.csv" % spider.name
            if not os.path.isfile(output_path):
                sorted_df.to_csv("%s_items_final.csv" % spider.name, index=False)
            else:
                sorted_df.to_csv(
                    "%s_items_final.csv" % spider.name,
                    mode="a",
                    encoding="utf-8-sig",
                    header=False,
                    index=False,
                )
            dedup = pd.read_csv("%s_items_final.csv" % spider.name, skiprows=0)
            final_data = dedup.drop_duplicates()
            final_data.to_csv("%s_items_final.csv" % spider.name, index=False)

            os.remove("%s_items_raw.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Csv pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s_items_raw.csv" % spider.name)


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


class LuluEgyptCsvPipeline(object):
    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        # self.file = open("%s-items.csv" % spider.name, "ab", newline="")
        self.items = []
        self.colnames = []

    def process_item(self, item, spider):
        # add the new fields
        for f in item.keys():
            if f not in self.colnames:
                self.colnames.append(f)

        # add the item itself to the list
        self.items.append(item)
        return item

    def spider_closed(self, spider):
        items_csv = os.path.isfile("%s_items.csv" % spider.name)
        with open("%s_items.csv" % spider.name, "a", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.colnames)

            if not items_csv:
                writer.writeheader()

            for item in self.items:
                writer.writerow(item)
            with open(
                "%s_items.csv" % spider.name, "r", encoding="utf-8-sig"
            ) as duplicate_csv, open(
                "%s_items_raw.csv" % spider.name, "w", encoding="utf-8-sig"
            ) as out_file:
                seen = set()
                for line in duplicate_csv:
                    if line in seen:
                        continue

                    seen.add(line)
                    out_file.write(line)
        os.remove("%s_items.csv" % spider.name)

        try:
            df = pd.read_csv("%s_items_raw.csv" % spider.name, skiprows=0)
            drop_cols = df.drop(["image_urls", "path", "images"], axis=1)
            drop_dup = drop_cols.drop_duplicates(subset=["externallink"])
            sorted_df = drop_dup.sort_values(by=["catalog_uuid"])
            output_path = "%s_items_final.csv" % spider.name
            if not os.path.isfile(output_path):
                sorted_df.to_csv("%s_items_final.csv" % spider.name, index=False)
            else:
                sorted_df.to_csv(
                    "%s_items_final.csv" % spider.name,
                    mode="a",
                    encoding="utf-8-sig",
                    header=False,
                    index=False,
                )
            dedup = pd.read_csv("%s_items_final.csv" % spider.name, skiprows=0)
            final_data = dedup.drop_duplicates()
            final_data.to_csv("%s_items_final.csv" % spider.name, index=False)

            os.remove("%s_items_raw.csv" % spider.name)
        except pd.errors.EmptyDataError:
            spider.log("Csv pipeline No columns to parse from file", logging.ERROR)
            os.remove("%s_items_raw.csv" % spider.name)


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
