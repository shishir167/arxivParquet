import xml.etree.ElementTree as ET
import os
import collections
import codecs, sys
from datetime import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *

#stores all authors and their unique names and ids
dictAuthors = collections.OrderedDict()

#lists with nodes and edges
NodeList = []
edgesList = []

#stores all nodes and edges in dictionary to avoid duplicates
dictEdges = collections.OrderedDict()
dictNodes = {}

idNo = 1

conf = SparkConf().setAppName("HelloSpark").setMaster("local[2]")
sc = SparkContext(conf=conf)
logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
sqlContext = SQLContext(sc)

def main():
    counter = 1
    #reading all xml files(the files were downloaded as 1.xml, 2.xml, 3.xml ...)
    filename = "./xml/" + str(counter) + ".xml"
    fd = open(filename, 'r')
    xmlResult = fd.read()
    print ("[info] processing xml files...")
    parseResults(xmlResult)
    fd.close()
    counter = 0
    # while counter != 5:
    while xmlResult:
        counter += 1
        filename = "./xml/" + str(counter) + ".xml"
        try:
            fd = open(filename, 'r')
            xmlResult = fd.read()
            fd.close()
        except:
            break
        parseResults(xmlResult)
    print ("[info] writing node files...")
    writeRecordsNodes()
    print ("[info] writing edges files...")
    writeRecordsEdges()

def writeRecordsNodes():
    return
    df = sqlContext.createDataFrame(NodeList, ["vid", "name", "affiliation", "estart", "eend"])
    df.saveAsParquetFile('./arxivNodes.paraquet')

def writeRecordsEdges():
    df = sqlContext.createDataFrame(edgesList, ["vid1","vid2","title","journal","estart","eend"])
    df.saveAsParquetFile('./arxivEdges.paraquet')

def processAuthorNodes(authors, pubYear):
    global dictAuthors;
    global idNo;

    for auth in authors:
        if not ((auth is None) or (pubYear is None)):
            affiliation = ''
            if len(auth) == 2:
                author = auth[1].text + " " + auth[0].text;
            elif len(auth) == 3:
                if "affiliation" in auth[2].tag:
                    affiliation = auth[2].text
                    author = auth[1].text + " " + auth[0].text;
                else:
                    author = auth[1].text + " " + auth[0].text + " " + auth[2].text;
            else:
                author = auth[0].text;
            date = pubYear.text
            dateEnd = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1)
            dateEnd = dateEnd.strftime('%Y-%m-%d')
            year = int(pubYear.text.split('-')[0]);

            auth_key_template = "{} {} {} {}"

            # if author does not exist in the dictionary, give id and add it
            if not author in dictAuthors:
                dictAuthors.update({author : idNo})
                authKey = auth_key_template.format(idNo, author, date, dateEnd)
                dictNodes.update({authKey : [year]})
                NodeList.append([idNo, author, affiliation, date, dateEnd])
                idNo += 1;
            #else just add it to the list
            else:
                authKey = auth_key_template.format(dictAuthors[author], author, date, dateEnd)
                if not authKey in dictNodes:
                    dictNodes.update({authKey : [year]})
                    NodeList.append([dictAuthors[author], author, affiliation, date, dateEnd])


def processAuthorEdges(authors, pubYear, journalReference, paperTitle):
    if len(authors) < 2: #error checking, return if only one author for a publication
        return;
    global dictAuthors;
    global dictEdges;
    index = 1;
    numAuthors = len(authors);
    auth_key_template = "{} {} {} {}"

    journalRef = ''
    if (journalReference is None):
        journalRef = ''
    else:
        journalRef = journalReference.text

    title = ''
    if (paperTitle is None):
        title = ''
    else:
        title = paperTitle.text

    for auth in authors:
        if not ((auth is None) or (pubYear is None)):
            if len(auth) == 2:
                author = auth[1].text + " " + auth[0].text;
            elif len(auth) == 3:
                if "affiliation" in auth[2].tag:
                    author = auth[1].text + " " + auth[0].text;
                else:
                    author = auth[1].text + " " + auth[0].text + " " + auth[2].text;
            else:
                author = auth[0].text;
            date = pubYear.text
            dateEnd = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1)
            dateEnd = dateEnd.strftime('%Y-%m-%d')
            year = int(pubYear.text.split('-')[0]);
            firstAuth = "";
            secondAuth = "";

            for num in range(index, numAuthors): #iterate through all co authors
                if len(authors[num]) == 2:
                    coAuthor = authors[num][1].text + " " + authors[num][0].text;
                elif len(authors[num]) == 3:
                    if "affiliation" in authors[num][2].tag:
                        coAuthor = authors[num][1].text + " " + authors[num][0].text;
                    else:
                        coAuthor = authors[num][1].text + " " + authors[num][0].text + " " + authors[num][2].text;
                else:
                    coAuthor = authors[num][0].text;

                if(author > coAuthor): #auth comes behind alphabetically
                    firstAuth = coAuthor;
                    secondAuth = author;
                else:
                    firstAuth = author;
                    secondAuth = coAuthor;

                authKey = auth_key_template.format(dictAuthors[firstAuth], dictAuthors[secondAuth], date, dateEnd)
                if not authKey in dictEdges: # check if edge record already exists
                    edgesList.append([dictAuthors[firstAuth], dictAuthors[secondAuth], title, str(journalRef.encode('utf-8'), 'utf-8').replace("\n", ""), date, dateEnd])
                    dictEdges.update({authKey : [year]});
                # else:
                    # if not year in dictEdges[authKey]:# prevent duplicate publication years
                    #     dictEdges[authKey].append(year); #append year to publication list
                    #     print("should we get here?")
                    #     print(authKey, dictEdges[authKey])
        index = index + 1;

def parseResults(parseString):
    root = ET.fromstring(parseString)
    records = root.iter("{http://arxiv.org/OAI/arXiv/}arXiv")

    for record in records:
        authors = record.find('{http://arxiv.org/OAI/arXiv/}authors');
        pubYear = record.find('{http://arxiv.org/OAI/arXiv/}created');
        processAuthorNodes(authors, pubYear);

    records = root.iter("{http://arxiv.org/OAI/arXiv/}arXiv")
    for record in records:
        authors = record.find('{http://arxiv.org/OAI/arXiv/}authors');
        pubYear = record.find('{http://arxiv.org/OAI/arXiv/}created');
        journalReference = record.find('{http://arxiv.org/OAI/arXiv/}journal-ref');
        title = record.find('{http://arxiv.org/OAI/arXiv/}title');
        processAuthorEdges(authors, pubYear, journalReference, title);

if __name__ == "__main__":
    main()