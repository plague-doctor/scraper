#!/bin/python

import scrapy
import os
import requests
import urllib.parse
import xml.etree.ElementTree as ET

from lxml import etree as ETE
from pprint import pprint
from scrapy.crawler import CrawlerProcess
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings


base_url = 'https://lubimyczytac.pl'
audioteka_url = 'https://audioteka.com/pl'
books = []


class results(scrapy.Item):
    authors = scrapy.Field()
    title = scrapy.Field()
    subtitle = scrapy.Field()
    narrators = scrapy.Field()
    publishedYear = scrapy.Field()
    publisher = scrapy.Field()
    isbn = scrapy.Field()
    genre = scrapy.Field()
    tag = scrapy.Field()
    rating = scrapy.Field()
    language = scrapy.Field()
    series = scrapy.Field()
    volume = scrapy.Field()
    description = scrapy.Field()
    image_lc = scrapy.Field()
    image_apl = scrapy.Field()


class audiobooks_spider(scrapy.Spider):
    name = 'books_spider'
    allowed_domains = ['lubimyczytac.pl']
    custom_settings = {
        'DOWNLOD_DELAY': 0,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
    }

    def start_requests(self):
        for b in self.my_books:
            self.start_urls.append(f"{base_url}/autorzy?phrase={b['author']}")
        return (scrapy.Request(url, callback=self.parse, meta={'download_timeout': 10}) for url in self.start_urls)

    def parse(self, response):
        self.logger.info('[parse] A response from %s just arrived!', response.url)
        link = f"{base_url}{response.css('a.authorAllBooks__singleTextAuthor::attr(href)').get()}"
        return scrapy.Request(link, callback=self.parse_books, meta={'download_timeout': 10})

    def parse_books(self, response):
        links = []
        test_links = []
        self.logger.info('[parse_books] A response from %s just arrived!', response.url)

        test_links = response.css("a.page-link.stdPaginator.btn:not([class*='ml-0'])::attr(href)").getall()

        if test_links:
            for link in test_links[1:]:
                links.append(f"{base_url}{link}")
        else:
            links.append(response.url)

        return (scrapy.Request(url, callback=self.parse_books_extra, dont_filter=True, meta={'download_timeout': 10}) for url in links)

    def parse_books_extra(self, response):
        links = []
        self.logger.info('[parse_books_extra] A response from %s just arrived!', response.url)
        author_tmp = response.css("h1::Text").get()
        author = author_tmp.strip()
        titles = response.css("a.authorAllBooks__singleTextTitle::Text").getall()
        urls = response.css("a.authorAllBooks__singleTextTitle::attr(href)").getall()


        for i in range(len(titles)):
            title = titles[i].strip()
            url = urls[i].strip()
            for b in self.my_books:
                if b['author'].lower() in author.lower():
                    for t in b['titles']:
                        for key, value in t.items():
                            if key.lower() == title.lower():
                                links.append(f"{base_url}{url}")
                                possition = value


        return (scrapy.Request(url, callback=self.parse_book_details, meta={'download_timeout': 10}) for url in links)

    def parse_book_details(self, response):
        self.logger.info('[parse_book_details] A response from %s just arrived!', response.url)

        series = ""
        volume = ""
        rating = ""

        for cykl in response.css("span.d-none.d-sm-block.mt-1").getall():
            html = ETE.HTML(cykl)
            text_1 = html.xpath("//span/text()")
            text_2 = html.xpath("//a/text()")
            if "Cykl" in text_1[0].strip():
                series = text_2[0].split('(tom')[0].strip()
                volume = text_2[0].split('(tom')[1].strip()[:-1]
        rating = response.css("span.big-number::Text").get().strip()
        rating = rating.replace(',', '.')

        m = results()
        m["authors"] = response.css("a.link-name.d-inline-block::Text").getall()
        m["title"] = get_clean(response.css("h1::Text").get(), dont_split=True)
        m["narrators"] = [""]
        m['publishedYear'] = ""
        m["publisher"] = ""
        m["isbn"] = ""
        m["genre"] = get_clean(response.css("a.book__category.d-sm-block.d-none::Text").get())
        m["tag"] = get_clean(response.css("a.tag::Text").getall())
        m['rating'] = rating
        m["language"] = "pol"
        m["series"] = series
        m["volume"] = volume
        m["description"] = get_text_block(response.css("div.collapse-content p::Text").getall())
        m["image_lc"] = get_clean(response.css("img.img-fluid::attr(src)").get(), dont_split=True)
        m["image_apl"] = ""

        for b in self.my_books:
            if b["author"] in m["authors"]:
                for t in b['titles']:
                    for key, value in t.items():
                        if key.lower() == m["title"].lower():
                            opf_file = f"{value}/metadata.opf"
                            opf_management(m, opf_file, key)

        yield m


class audioteka_spider(scrapy.Spider):
    name = 'audioteka_spider'
    allowed_domains = ['audioteka.com']
    custom_settings = {'DOWNLOD_DELAY': 3}

    def start_requests(self):
        for b in self.my_books:
            for t in b['titles']:
                for k in t.keys():
                    link = (f"{audioteka_url}/search?query={b['author']} {k}")
                    if link not in self.start_urls:
                        self.start_urls.append(link)
        return (scrapy.Request(url, callback=self.parse, meta={'download_timeout': 10}) for url in self.start_urls)

    def parse(self, response):
        self.logger.info('[parse] A response from %s just arrived!', response.url)
        rs = response.css('h2.item__title a').getall()
        for item in rs:
            html = ETE.HTML(item)
            title = html.xpath("//a/text()")
            link = html.xpath("//a/@href")
            if title[0] in urllib.parse.unquote(response.url):
                link = link[0]
                break
        return scrapy.Request(link, callback=self.parse_book_details, meta={'download_timeout': 10})

    def parse_book_details(self, response):
        self.logger.info('[parse_book_details] A response from %s just arrived!', response.url)
        li_1 = response.css('li span.text-label::Text').getall()
        li_2 = response.css('li span.text').getall()
        li_3 = []
        desc_list = []

        for item in li_2:
            html = ETE.HTML(item)
            lista_1 = html.xpath("//span/text()")
            lista_2 = html.xpath("//a/text()")
            if len(lista_1[0].strip()) > 0:
                li_3.append(lista_1)
            else:
                li_3.append(lista_2)

        index_narrator = li_1.index("Czyta:")
        index_author = li_1.index("Autor:")

        for item in response.css('div.product__desc div').getall():
            html = ETE.HTML(item)
            text = html.xpath("//div/text()")
            for line in text:
                line = line.strip()
                if len(line) > 0:
                    desc_list.append(f"{line}\n")

        m = results()
        m["authors"] = li_3[index_author]
        m["title"] = f"{response.css('h1.product-title::Text').get()}"
        m["narrators"] = li_3[index_narrator]
        m["publishedYear"] = ""
        m["publisher"] = ""
        m["isbn"] = ""
        m["genre"] = [""]
        m["tag"] = [""]
        m["rating"] = ""
        m["language"] = ""
        m["series"] = ""
        m["volume"] = ""
        m["description"] = get_text_block(desc_list)
        m["image_lc"] = ""
        m["image_apl"] = f"https:{response.css('div.product-image__img img::attr(src)').get()}"

        for b in self.my_books:
            away = False
            if b["author"] in m["authors"]:
                for t in b['titles']:
                    for key, value in t.items():
                        if key.lower() in m["title"].lower():
                            opf_file = f"{value}/metadata.opf"
                            opf_management(m, opf_file, key)
                            away = True
                            break
                if away:
                    break

        yield m


def get_clean(source, separator=',', dont_split=False):
    if type(source) is str:
        if dont_split:
            to_return = str()
            to_return = source.strip()
        else:
            list_tmp = source.split(separator)
            source = list_tmp

    if type(source) is list:
        to_return = list()
        for i in source:
            to_return.append(i.strip())

    return to_return


def get_text_block(lista):
    text_block = ' '.join(str(item) for item in lista)
    return text_block


def get_pure_title(t, s):
    title_l = t.split('-')
    if len(title_l) > 1:
        title_tmp = title_l[1].strip()
    else:
        title_tmp = title_l[0].strip()
    return {title_tmp: s}


def check_lists(list_1, list_2):
    check = False
    check = all(item in list_1 for item in list_2) and all(item in list_2 for item in list_1)
    return check


def get_image(image_file, image_url):
    r = requests.get(image_url, allow_redirects=True)
    open(image_file, 'wb').write(r.content)


def opf_management(book, opf, original_title):
    try:
        tree = ET.parse(opf)
        root = tree.getroot()
        metadata = root[0]

        xml_authors = list()
        xml_narrators = list()
        xml_subjects = list()
        xml_tags = list()

        xml_title = root[0].find('{http://purl.org/dc/elements/1.1/}title')
        xml_date = root[0].find('{http://purl.org/dc/elements/1.1/}date')
        xml_publisher = root[0].find('{http://purl.org/dc/elements/1.1/}publisher')
        xml_identifier = root[0].find('{http://purl.org/dc/elements/1.1/}identifier')
        xml_description = root[0].find('{http://purl.org/dc/elements/1.1/}description')
        xml_language = root[0].find('{http://purl.org/dc/elements/1.1/}language')
        xml_rating = root[0].find('{http://purl.org/dc/elements/1.1/}rating')

        xml_creator = root[0].findall('{http://purl.org/dc/elements/1.1/}creator')
        xml_subject = root[0].findall('{http://purl.org/dc/elements/1.1/}subject')
        xml_tag = root[0].findall('{http://purl.org/dc/elements/1.1/}tag')
        xml_meta = root[0].findall('{http://www.idpf.org/2007/opf}meta')

        for creator in xml_creator:
            for value in creator.attrib.values():
                if 'aut' in value:
                    xml_authors.append(creator.text)
                else:
                    xml_narrators.append(creator.text)
        for subject in xml_subject:
            xml_subjects.append(subject.text)
        for tag in xml_tag:
            xml_tags.append(tag.text)
        for meta in xml_meta:
            val_list = list(meta.attrib.values())
            if 'calibre:series_index' in val_list[0]:
                xml_volume = val_list[1]
            else:
                xml_series = val_list[1]

        if book['publishedYear'] != "" and xml_date.text != book['publishedYear']:
            xml_date.text = book['publishedYear']
        if book['publisher'] != "" and xml_publisher.text != book['publisher']:
            xml_publisher.text = book['publisher']
        if book['description'] != "" and xml_description.text != book['description']:
            xml_description.text = book['description']
        if book['isbn'] != "" and xml_identifier.text != book['isbn']:
            xml_identifier.text = book['isbn']
        if book['language'] != "" and xml_language.text != book['language']:
            xml_language.text = book['language']

        try:
            if book['rating'] != "" and xml_rating.text != book['rating']:
                xml_rating.text = book['rating']
        except Exception:
            ET.SubElement(metadata, "dc:rating").text = book['rating']
            pass

        if len(book['narrators'][0].strip()) > 0 and not check_lists(book['narrators'], xml_narrators):
            for creator in xml_creator:
                for value in creator.attrib.values():
                    if 'nrt' in value:
                        root[0].remove(creator)
            for nrt in book['narrators']:
                ET.SubElement(metadata, "dc:creator", {"ns0:role": "nrt"}).text = nrt

        if len(book['genre']) > 0 and not check_lists(book['genre'], xml_subjects):
            for subject in xml_subject:
                root[0].remove(subject)
            for genre in book['genre']:
                ET.SubElement(metadata, "dc:subject").text = genre

        if len(book['tag']) > 0 and not check_lists(book['tag'], xml_tags):
            for tag in xml_tag:
                root[0].remove(tag)
            for tag in book['tag']:
                ET.SubElement(metadata, "dc:tag").text = tag

        for meta in xml_meta:
            val_list = list(meta.attrib.values())
            if val_list[0] == "calibre:series_index" \
                    and val_list[1] != book['volume']:
                    meta.set("content", book['volume'])

            elif val_list[0] == "calibre:series" \
                    and val_list[1] != book['series']:
                        meta.set("content", book['series'])

    except IOError:
        root = ET.Element("package", xmlns="http://www.idpf.org/2007/opf", version="2.0")
        metadata = ET.SubElement(root, "metadata", {"xmlns:dc": "http://purl.org/dc/elements/1.1/", "xmlns:opf": "http://www.idpf.org/2007/opf"})
        ET.SubElement(metadata, "dc:title").text = original_title
        for i in book['authors']:
            ET.SubElement(metadata, "dc:creator", {"opf:role": "aut"}).text = i

        for i in book['narrators']:
            ET.SubElement(metadata, "dc:creator", {"opf:role": "nrt"}).text = i

        ET.SubElement(metadata, "dc:date").text = book['publishedYear']
        ET.SubElement(metadata, "dc:publisher").text = book['publisher']
        ET.SubElement(metadata, "dc:identifier", {"opf:scheme": "ISBN"}).text = book['isbn']
        ET.SubElement(metadata, "dc:description").text = book['description']
        ET.SubElement(metadata, "dc:language").text = book['language']
        ET.SubElement(metadata, "dc:rating").text = book['rating']
        for i in book['genre']:
            ET.SubElement(metadata, "dc:subject").text = i
        for i in book['tag']:
            ET.SubElement(metadata, "dc:tag").text = i
        ET.SubElement(metadata, "meta", {"name": "calibre:series", "content": f"{book['series']}"})
        ET.SubElement(metadata, "meta", {"name": "calibre:series_index", "content": f"{book['volume']}"})

        tree = ET.ElementTree(root)
        pass
    finally:
        if book['image_lc'] != "" and not os.path.exists(f"{os.path.dirname(opf)}/cover.{book['image_lc'].split('.')[-1]}"):
            img_file = os.path.dirname(opf)+f"/cover.{book['image_lc'].split('.')[-1]}"
            get_image(img_file, book['image_lc'])
        if book['image_apl'] != "" and not os.path.exists(f"{os.path.dirname(opf)}/cover.{book['image_apl'].split('.')[-1]}"):
            img_file = os.path.dirname(opf)+f"/cover.{book['image_apl'].split('.')[-1]}"
            get_image(img_file, book['image_apl'])

        ET.indent(tree, space="\t", level=0)
        tree.write(opf, xml_declaration=True, encoding='utf-8')
        print(f"============== SAVED: {opf}")

    return


def filter_books(data):
    books = []
    for index, line in enumerate(data):
        sections = line.split("/")
        length = len(sections)
        # is a book in a series
        if length > 3:
            books.append(line)
        # could be a book or a series title
        elif length == 3:
            # the last entry so must be a book
            if index == len(data)-1:
                books.append(line)
            # last entry is not a book in a series so second last line must be a book
            elif len(data[index+1].split("/")) < 3:
                books.append(line)
            # next line does not contain the same series title section so this line must be a single book
            elif sections[2] !=data[index+1].split("/")[2]:
                books.append(line)
    return books


def main():
    global books

    entry = {}
    author = ""
    title1 = {}
    title2 = {}
    titles = []
    series = []

    subdirs = filter_books([x[0] for x in os.walk('.')])

    for s in subdirs:
        _dir = s.split('/')[1:]

        if author == "":
            author = _dir[0]

        if author != _dir[0]:
            entry['author'] = author
            entry['titles'] = titles
            books.append(entry)

            author = _dir[0]

            entry = {}
            title1 = {}
            title2 = {}
            titles = []
            series = []

        title1 = get_pure_title(_dir[1], s)
        titles.append(title1)

        if len(_dir) == 3:
            title2 = get_pure_title(_dir[2], s)
            if title1 in titles:
                titles.remove(title1)
            if _dir[1] not in series:
                series.append(_dir[1])
            if title2 not in titles:
                titles.append(title2)

    entry['author'] = author
    entry['titles'] = titles
    books.append(entry)

    # pprint(books)

    configure_logging()
    settings = get_project_settings()
    runner = CrawlerRunner(settings)
    runner.crawl(audioteka_spider, my_books=books)
    runner.crawl(audiobooks_spider, my_books=books)
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()


if __name__ == "__main__":
    main()
