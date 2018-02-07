from helmspoint.pipeline import Pipeline
from helmspoint.stage import Stage
import csv

# streaming should yield or call success or fail

# @datatype("string", "[string]")
# @batch
def load_csv(filename):
    result = []

    with open(filename, 'r') as f:
        reader = csv.reader(f)
        for record in reader:
            result.append(record)
    return result

# @datatype("[string]", "[string]")
# @batch
def fetch_all(urls):
    def get_page(pages, url):
        result = http.get(url)
        if result.status == 200:
            return result.body
        else:
            return ""

    pages = reduce(get_page, urls, [])

    return pages

# @datatype("[string]", "[{ string: int }]")
# @batch
def word_count(pages):
    counts = {}
    for page in pages:
        for token in page.split("\s+"):
            if counts[token] != None:
                counts[token] += 1
            else:
                counts[token] = 1
        return counts

# @datatype("[{ string: int }]", "[{ string: int }]")
# @batch
def word_count_merge(counts):
    total_count = {}
    for count in counts:
        total_count = total_count.merge(count)

    return total_count

# TODO using names to reference kinda sucks
p = Pipeline()
p.append(load_csv)
p.append(fetch_all, ["load_csv"])
p.append(word_count, ["fetch_all"])
p.append(word_count_merge, ["word_count"])

p.status()