package io.anserini.query_match;

import org.kohsuke.args4j.Option;

public class BaseMatcherArgs {
  @Option(name = "-collection", metaVar = "[class]", required = true, usage = "Collection class in io.anserini.collection.")
  public String collectionClass;

  @Option(name = "-input", metaVar = "[path]", required = true, usage = "Input collection.")
  public String input;

  @Option(name = "-index", metaVar = "[path]", required = true, usage = "Path to Lucene index")
  public String index;

  @Option(name = "-stemmer", usage = "Stemmer: one of the following porter,krovetz,none. Default porter")
  public String stemmer = "porter";

  @Option(name = "-keepStopwords", usage = "Boolean switch to keep stopwords in the query topics")
  public boolean keepStopwords = false;

  @Option(name = "-stopwords", metaVar = "[file]", forbids = "-keepStopwords", usage = "Path to file with stopwords.")
  public String stopwords = null;

  @Option(name = "-queryGenerator", metaVar = "[class]", usage = "QueryGenerator to use.")
  public String queryGenerator = "BagOfWordsQueryGenerator";

  @Option(name = "-documentGenerator", metaVar = "[class]",
      usage = "Document generator class in package 'io.anserini.index.generator'.")
  public String documentGenerator = "DefaultLuceneDocumentGenerator";

  @Option(name = "-outputMatchesFile", metaVar = "[file]", required = true, usage = "Path to output file to contain matches.")
  public String outputMatchesFile = null;

  @Option(name = "-outputPerformanceDataFile", metaVar = "[file]", required = true, usage = "Path to output file to contain performance data.")
  public String perfomanceDataFile = null;
}
