package io.anserini.query_index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.ParserProperties;

public class QueryIndexCollection<K extends Comparable<K>> extends AbstractIndexer<K> {

  public QueryIndexCollection(Args args) throws IOException {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    QueryIndexCollection.Args indexArgs = new QueryIndexCollection.Args();
    CmdLineParser parser = new CmdLineParser(indexArgs,
        ParserProperties.defaults().withUsageWidth(120));

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      if (indexArgs.options) {
        System.err.printf("Options for %s:\n\n", QueryIndexCollection.class.getSimpleName());
        parser.printUsage(System.err);

        List<String> required = new ArrayList<>();
        parser.getOptions().forEach((option) -> {
          if (option.option.required()) {
            required.add(option.option.toString());
          }
        });

        System.err.printf("\nRequired options are %s\n", required);
      } else {
        System.err.printf(
            "Error: %s. For help, use \"-options\" to print out information about options.\n",
            e.getMessage());
      }

      return;
    }

    new QueryIndexCollection(indexArgs).run();
    System.exit(0);
  }
}
