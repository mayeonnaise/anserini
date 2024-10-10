package io.anserini.query_match;

import io.anserini.analysis.DefaultEnglishAnalyzer;
import io.anserini.collection.SourceDocument;
import io.anserini.index.Constants;
import io.anserini.index.generator.LuceneDocumentGenerator;
import io.anserini.search.query.QueryGenerator;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.monitor.Monitor;
import org.apache.lucene.monitor.MonitorConfiguration;
import org.apache.lucene.monitor.MonitorQuerySerializer;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

public final class MatchQueryCollection extends BaseMatcher implements Runnable, Closeable {

  private static final Logger LOG = LogManager.getLogger(MatchQueryCollection.class);

  public static class Args extends BaseMatcherArgs {
    @Option(name = "-options", usage = "Print information about options.")
    public Boolean options = false;
  }

  @SuppressWarnings("unchecked")
  public MatchQueryCollection(Args args) throws IOException {
    super(args);
    Path indexPath = Path.of(args.index);

    LOG.info("============ Initializing Matcher ============");
    LOG.info("Index: " + indexPath);

    try {
      this.queryGenerator = (QueryGenerator) Class.forName(
          "io.anserini.search.query." + args.queryGenerator).getConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Unable to load query generator \"%s\".", args.queryGenerator));
    }

    try {
      super.documentGenerator = (Class<LuceneDocumentGenerator<? extends SourceDocument>>)
          Class.forName("io.anserini.index.generator." + args.documentGenerator);
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Unable to load document generator class \"%s\".", args.documentGenerator));
    }

    Analyzer analyzer = getAnalyzer();
    MonitorConfiguration monitorConfiguration = new MonitorConfiguration();
    monitorConfiguration.setIndexPath(Paths.get(args.index), MonitorQuerySerializer.fromParser(
        queryString -> queryGenerator.buildQuery(Constants.CONTENTS, analyzer, queryString)));

    this.monitor = new Monitor(analyzer, monitorConfiguration);
  }

  private Analyzer getAnalyzer() {
    // Default to English
    LOG.info("Using DefaultEnglishAnalyzer");
    LOG.info("Stemmer: " + args.stemmer);
    LOG.info("Keep stopwords? " + args.keepStopwords);
    LOG.info("Stopwords file: " + args.stopwords);
    try {
      return DefaultEnglishAnalyzer.fromArguments(args.stemmer, args.keepStopwords, args.stopwords);
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void run() {
    match();
  }

  public static void main(String[] args) {
    Args searchArgs = new Args();
    CmdLineParser parser = new CmdLineParser(searchArgs, ParserProperties.defaults().withUsageWidth(120));

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      if (searchArgs.options) {
        System.err.printf("Options for %s:\n\n", MatchQueryCollection.class.getSimpleName());
        parser.printUsage(System.err);

        List<String> required = new ArrayList<>();
        parser.getOptions().forEach((option) -> {
          if (option.option.required()) {
            required.add(option.option.toString());
          }
        });

        System.err.printf("\nRequired options are %s\n", required);
      } else {
        System.err.printf("Error: %s. For help, use \"-options\" to print out information about options.\n", e.getMessage());
      }

      return;
    }

    final long start = System.nanoTime();

    try (MatchQueryCollection searcher = new MatchQueryCollection(searchArgs)) {
      searcher.run();
    } catch (IllegalArgumentException | IOException e) {
      System.err.printf("Error: %s\n", e.getMessage());
    }

    final long durationMillis = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    LOG.info("Total run time: " + DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss"));
  }
}
