package io.anserini.query_index;

import io.anserini.analysis.DefaultEnglishAnalyzer;
import io.anserini.index.Constants;
import io.anserini.search.query.QueryGenerator;
import io.anserini.search.topicreader.TopicReader;
import io.anserini.search.topicreader.Topics;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.monitor.Monitor;
import org.apache.lucene.monitor.MonitorConfiguration;
import org.apache.lucene.monitor.MonitorQuery;
import org.apache.lucene.monitor.MonitorQuerySerializer;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

public abstract class AbstractIndexer<K extends Comparable<K>> implements Runnable {

  private static final Logger LOG = LogManager.getLogger(
      io.anserini.query_index.AbstractIndexer.class);

  public static class Args {

    @Option(name = "-options", usage = "Print information about options.")
    public Boolean options = false;

    @Option(name = "-topics", metaVar = "[file]", handler = StringArrayOptionHandler.class, required = true, usage = "topics file")
    public String[] topics;

    @Option(name = "-index", metaVar = "[path]", required = true, usage = "Index path.")
    public String index;

    @Option(name = "-topicReader", metaVar = "[class]", usage = "TopicReader to use.")
    public String topicReader;

    @Option(name = "-topicField", usage =
        "Which field of the query should be used, default \"title\"."
            + " For TREC ad hoc topics, description or narrative can be used.")
    public String topicField = "title";

    @Option(name = "-stemmer", usage = "Stemmer: one of the following porter,krovetz,none. Default porter")
    public String stemmer = "porter";

    @Option(name = "-keepStopwords", usage = "Boolean switch to keep stopwords in the query topics")
    public boolean keepStopwords = false;

    @Option(name = "-stopwords", metaVar = "[file]", forbids = "-keepStopwords", usage = "Path to file with stopwords.")
    public String stopwords = null;

    @Option(name = "-generator", metaVar = "[class]", usage = "QueryGenerator to use.")
    public String queryGenerator = "BagOfWordsQueryGenerator";
  }

  protected final Args args;
  protected Monitor monitor;
  protected final SortedMap<K, Map<String, String>> topics;
  protected QueryGenerator queryGenerator;
  protected Analyzer analyzer;

  @SuppressWarnings("unchecked")
  public AbstractIndexer(Args args) throws IOException {
    this.args = args;

    LOG.info("============ Loading Index Configuration ============");
    LOG.info("AbstractIndexer settings:");
    LOG.info(" + Index path: " + args.index);

    topics = new TreeMap<>();
    for (String topicsFile : args.topics) {
      Path topicsFilePath = Paths.get(topicsFile);
      if (!Files.exists(topicsFilePath) || !Files.isRegularFile(topicsFilePath)
          || !Files.isReadable(topicsFilePath)) {
        Topics ref = Topics.getByName(topicsFile);
        if (ref == null) {
          throw new IllegalArgumentException(
              String.format("\"%s\" does not refer to valid topics.", topicsFilePath));
        } else {
          topics.putAll(TopicReader.getTopics(ref));
        }
      } else {
        if (args.topicReader == null) {
          throw new IllegalArgumentException("Must specify the topic reader using -topicReader.");
        }

        try {
          TopicReader<K> tr = (TopicReader<K>) Class.forName(
                  String.format("io.anserini.search.topicreader.%sTopicReader", args.topicReader))
              .getConstructor(Path.class).newInstance(topicsFilePath);

          topics.putAll(tr.read());
        } catch (Exception e) {
          throw new IllegalArgumentException(
              String.format("Unable to load topic reader \"%s\".", args.topicReader));
        }
      }
    }

    this.analyzer = DefaultEnglishAnalyzer.fromArguments(args.stemmer, args.keepStopwords,
        args.stopwords);

    try {
      this.queryGenerator = (QueryGenerator) Class.forName(
          "io.anserini.search.query." + args.queryGenerator).getConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Unable to load query generator \"%s\".", args.queryGenerator));
    }

    MonitorConfiguration monitorConfiguration = new MonitorConfiguration();
    monitorConfiguration.setIndexPath(Paths.get(args.index), MonitorQuerySerializer.fromParser(
        queryString -> queryGenerator.buildQuery(Constants.CONTENTS, analyzer, queryString)));

    this.monitor = new Monitor(this.analyzer, monitorConfiguration);
  }

  @Override
  public void run() {
    LOG.info("============ Indexing Collection ============");
    final long start = System.nanoTime();

    List<MonitorQuery> monitorQueries = topics.entrySet().stream().map(entry -> {
      K queryId = entry.getKey();
      Map<String, String> queryFields = entry.getValue();
      String queryString = queryFields.get(args.topicField);

      return new MonitorQuery(queryId.toString(),
          this.queryGenerator.buildQuery(Constants.CONTENTS, analyzer, queryString), queryString,
          new HashMap<>());
    }).collect(Collectors.toList());

    try {
      monitor.register(monitorQueries);
    } catch (IOException e) {
      LOG.error(e);
    }

    LOG.info(String.format("Indexing Complete! %,d queries indexed", monitorQueries.size()));

    final long durationMillis = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start,
        TimeUnit.NANOSECONDS);
    LOG.info(String.format("Total %,d queries indexed in %s", monitorQueries.size(),
        DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss")));
  }
}
