from helmspoint.pipeline import Pipeline
import urllib.request
import re
import os
from bs4 import BeautifulSoup

# In parallel streaming, a single stage can generate lots of data with one run,
# so the rates in which they're running is all different, which means the
# buffering and worker pool requirements are different for all stages.
# @stream
def load_text_stream(filepath):
    with open(filepath, 'r') as f:
        for line in f:
            success(line)

# ------------

def text_source():
    return "datasource/pride_prejudice_urls.txt"

# batching it requires we fit the data into memory
# @batch
def load_text_batch(filepath):
    print("**** load_text_batch %s" % filepath)

    urls = []
    with open(filepath, 'r') as f:
        for line in f:
            print(line)
            urls.append(line)

    return urls

# NOTE How do we say process this in parallel by splitting the input for workers?
# @datatype("[string]", "[string]")
# @batch
def fetch_all(urls):
    def get_page(url):
        try:
            return urllib.request.urlopen(url).read()
        except urllib.error.HTTPError as err:
            return ""

    print("**** fetching pages")
    pages = []
    for url in urls:
        html = get_page(url)
        print("  fetched %s" % url)
        pages.append(str(html))

    return pages

# @datatype("[string]", "[string]")
# @batch
def soup_parse(pages):
    def get_paragraphs(page):
        soup = BeautifulSoup(page, "html5lib")
        return " ".join([p.text for p in soup.find_all('p')])

    print("**** soup parsing")
    texts = []
    for page in pages:
        paragraph = get_paragraphs(page)
        print("  get text in page... %s" % page[0:100])
        texts.append(paragraph)

    return texts

# NOTE We'd also like to be able to process this in parallel. does it need to be
# synced with the previous stage?
# @datatype("[string]", "[{ string: int }]")
# @batch
def word_count(texts):
    print("**** word counting")
    counts = {}
    for text in texts:
        for token in re.compile("([^\w]|[\\\\n])+").split(text):
            token = token.lower()
            if token in counts:
                counts[token] += 1
            else:
                counts[token] = 1
        print("  counted %s unique tokens" % len(counts.keys()))

    return counts

# NOTE not unused for serial processing. How to say we join the previous
# parallel stages of workers into this single one?
# @datatype("[{ string: int }]", "[{ string: int }]")
# @batch
def word_count_merge(counts, tokens):
    total_count = {}
    for count in counts:
        total_count = total_count.merge(count)

    return total_count


p = Pipeline()
p.append(text_source)
p.append(load_text_batch, filepath = "text_source")
p.append(fetch_all, urls = "load_text_batch")
p.append(soup_parse, pages = "fetch_all")
p.append(word_count, texts = "soup_parse")

p.status()

p.run()