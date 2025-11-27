import lucene
import csv
import os
import shutil
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader, Term
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.document import Document, TextField, StringField, Field
from org.apache.lucene.search import IndexSearcher, TermQuery, WildcardQuery
from org.apache.lucene.queryparser.classic import QueryParser
from java.nio.file import Paths
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser
from org.apache.lucene.search import BooleanQuery, BooleanClause, FuzzyQuery


lucene.initVM(vmargs=['-Djava.awt.headless=true'])


TSV_FILE = "dino_joined_output.tsv"
INDEX_DIR = "lucene_dino_index"

# CREATE INDEX

def create_index():
    print("=" * 80)
    print("CREATE INDEX")
    print("=" * 80)

    if os.path.exists(INDEX_DIR):
        shutil.rmtree(INDEX_DIR)

    directory = FSDirectory.open(Paths.get(INDEX_DIR))
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)

    writer = IndexWriter(directory, config)
    doc_count = 0

    with open(TSV_FILE, "r", encoding="utf-8") as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter="\t")

        for row in reader:
            doc = Document()

            def add_text_field(field):
                val = row.get(field, "").strip()
                if val:
                    doc.add(TextField(field, val, Field.Store.YES))

            def add_exact_field(field):
                val = row.get(field, "").strip()
                if val:
                    doc.add(StringField(field, val, Field.Store.YES))

            add_text_field("name")
            add_exact_field("area")
            add_exact_field("time_period")
            add_text_field("classification")
            add_text_field("description")
            add_exact_field("length")
            add_exact_field("weight")
            add_exact_field("wingspan")
            add_exact_field("fossil_range_wiki")
            add_text_field("description_wiki")
            add_text_field("discovery_wiki")
            add_text_field("classification_wiki")
            add_exact_field("source_link")

            writer.addDocument(doc)
            doc_count += 1

    writer.commit()
    writer.close()

    print(f"Indexed documents: {doc_count}")

# EXACT SEARCH

def search(query_text, field="name"):
    directory = FSDirectory.open(Paths.get(INDEX_DIR))
    reader = DirectoryReader.open(directory)
    searcher = IndexSearcher(reader)
    analyzer = StandardAnalyzer()

    if field in {"area", "time_period", "length", "weight", "wingspan"}:
        query = TermQuery(Term(field, query_text))
    else:
        parser = QueryParser(field, analyzer)
        query = parser.parse(query_text)

    results = searcher.search(query, 10)

    print(f"\nField: {field}")
    print(f"Query: {query_text}")
    print(f"Hits: {results.totalHits.value()}\n")

    for i, hit in enumerate(results.scoreDocs, 1):
        doc = searcher.storedFields().document(hit.doc)
        print(f"{i}. {doc.get('name')} | Source: {doc.get('source_link')} | Score: {hit.score:.2f}")

    reader.close()

# WILDCARD SEARCH

def wildcard_search(pattern):
    directory = FSDirectory.open(Paths.get(INDEX_DIR))
    reader = DirectoryReader.open(directory)
    searcher = IndexSearcher(reader)

    query = WildcardQuery(Term("name", pattern))
    results = searcher.search(query, 20)

    print(f"\nWildcard: {pattern}")
    print(f"Hits: {results.totalHits.value()}\n")

    for i, hit in enumerate(results.scoreDocs, 1):
        doc = searcher.storedFields().document(hit.doc)
        print(f"{i}. {doc.get('name')} | Source: {doc.get('source_link')} | Score: {hit.score:.2f}")

    reader.close()

# SMART SEARCH

def smart_search(user_query):
    directory = FSDirectory.open(Paths.get(INDEX_DIR))
    reader = DirectoryReader.open(directory)
    searcher = IndexSearcher(reader)
    analyzer = StandardAnalyzer()

    fields = ["name", "classification", "description", "description_wiki", "classification_wiki"]

    boosts = {
        "name": 5.0,
        "classification": 1.0,
        "description": 2.0,
        "description_wiki": 2.0,
        "classification_wiki": 1.0
    }

    parser = MultiFieldQueryParser(fields, analyzer)
    parser.setAllowLeadingWildcard(True)           
    # Boolean operator
    parser.setDefaultOperator(QueryParser.Operator.OR) 
    # Boost
    # try:
    #     parser.setBoosts(boosts)
    # except Exception:
    #     pass

    main_query = MultiFieldQueryParser.parse(parser, user_query)

    fuzzy_builder = BooleanQuery.Builder()
    for word in user_query.lower().split():
        if len(word) > 3:
            fuzzy_query = FuzzyQuery(Term("name", word), 2)
            fuzzy_builder.add(fuzzy_query, BooleanClause.Occur.SHOULD)

    final_query = BooleanQuery.Builder() \
        .add(main_query, BooleanClause.Occur.SHOULD) \
        .add(fuzzy_builder.build(), BooleanClause.Occur.SHOULD) \
        .build()

    results = searcher.search(final_query, 10)

    print(f"\nQuery: {user_query}")
    print(f"Hits: {results.totalHits.value()}\n")
    for i, hit in enumerate(results.scoreDocs, 1):
        doc = searcher.storedFields().document(hit.doc)
        print(f"{i}. {doc.get('name')} | Source: {doc.get('source_link')} | Score: {hit.score:.2f}")

    reader.close()


# MAIN

if __name__ == "__main__":
    create_index()

    print("=" * 80)
    print("SEARCH TESTS")
    print("=" * 80)

    search("Triceratops", "name")
    search("Europe", "area")
    search("72.1–66 Ma", "time_period")

    print("=" * 80)
    print("WILDCARD TEST")
    print("=" * 80)

    wildcard_search("tri*")

    print("=" * 80)
    print("SMART SEARCH TESTS")
    print("=" * 80)

    smart_search("biggest dinosaur europe")
    # typo 
    smart_search("tricerato")
    # wildcard              
    smart_search("tri*")
    # wildcard at the start
    smart_search("*ceratops")                    
    smart_search("late jurassic giant")


