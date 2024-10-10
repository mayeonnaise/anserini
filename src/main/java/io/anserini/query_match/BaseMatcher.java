package io.anserini.query_match;

import io.anserini.collection.DocumentCollection;
import io.anserini.collection.FileSegment;
import io.anserini.collection.SourceDocument;
import io.anserini.index.generator.LuceneDocumentGenerator;
import io.anserini.search.query.QueryGenerator;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.monitor.MatchingQueries;
import org.apache.lucene.monitor.Monitor;

import java.nio.file.Path;
import org.apache.lucene.monitor.ScoringMatch;
import org.apache.lucene.search.similarities.BM25Similarity;

public class BaseMatcher {
    private static final Logger LOG = LogManager.getLogger(BaseMatcher.class);

    protected final BaseMatcherArgs args;
    protected Path collectionPath;
    protected Path outputMatchesPath;
    protected Path performanceDataPath;
    protected DocumentCollection<? extends SourceDocument> collection;
    protected Class<LuceneDocumentGenerator<? extends SourceDocument>> documentGenerator;
    protected AtomicLong numMatched = new AtomicLong();
    protected QueryGenerator queryGenerator;
    protected Monitor monitor;

    @SuppressWarnings("unchecked")
    public BaseMatcher(BaseMatcherArgs args) {
        this.args = args;

        LOG.info("============ Loading Matcher Configuration ============");
        LOG.info("BaseMatcher settings:");
        LOG.info(" + DocumentCollection path: " + args.input);
        LOG.info(" + CollectionClass: " + args.collectionClass);
        LOG.info(" + Index path: " + args.index);

        // Our documentation uses /path/to/foo as a convention: to make copy and paste of the commands work,
        // we assume collections/ as the path location.
        String inputPathStr = args.input;
        if (inputPathStr.startsWith("/path/to")) {
            inputPathStr = inputPathStr.replace("/path/to", "collections");
        }
        this.collectionPath = Paths.get(inputPathStr);
        if (!Files.exists(collectionPath) || !Files.isReadable(collectionPath) || !Files.isDirectory(collectionPath)) {
            throw new IllegalArgumentException(String.format("Invalid collection path \"%s\".", collectionPath));
        }

        this.outputMatchesPath = Paths.get(args.outputMatchesFile);

        this.performanceDataPath = Paths.get(args.perfomanceDataFile);

        try {
            Class<? extends DocumentCollection<?>> collectionClass = (Class<? extends DocumentCollection<?>>)
                Class.forName("io.anserini.collection." + args.collectionClass);
            this.collection = collectionClass.getConstructor(Path.class).newInstance(collectionPath);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Unable to load collection class \"%s\".", args.collectionClass));
        }
    }

    public void match() {
        LOG.info("============ Matching Collection ============");
        final long start = System.nanoTime();

        final List<Path> segmentPaths = collection.getSegmentPaths();

        segmentPaths.forEach((segmentPath) -> {
            try {
                @SuppressWarnings("unchecked")
                LuceneDocumentGenerator<SourceDocument> generator = (LuceneDocumentGenerator<SourceDocument>)
                    documentGenerator.getDeclaredConstructor((Class<?>[]) null).newInstance();

                MatcherThread thread = new MatcherThread(this.monitor, segmentPath,
                    outputMatchesPath, performanceDataPath, generator,
                    numMatched);
                thread.start();

                try {
                    thread.join();
                } catch (InterruptedException ie) {
                    thread.interrupt();
                }

            } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                     NoSuchMethodException e) {
                throw new IllegalArgumentException(
                    String.format("Unable to load LuceneDocumentGenerator \"%s\".",
                        documentGenerator.getSimpleName()));
            }
        });

        final long durationMillis = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        LOG.info(String.format("Total %,d documents matched in %s", numMatched.get(),
            DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss")));
    }

    public class MatcherThread extends Thread {
        private final Path inputFile;
        private final File outputMatchesFile;
        private final File performanceDataFile;
        private final LuceneDocumentGenerator<SourceDocument> generator;
        private final Monitor monitor;
        private AtomicLong numMatched;

        public MatcherThread(Monitor monitor, Path inputFile, Path outputMatchesPath, Path performanceDataPath, LuceneDocumentGenerator<SourceDocument> generator, AtomicLong numMatched) {
            this.monitor = monitor;
            this.inputFile = inputFile;
            this.outputMatchesFile = outputMatchesPath.toFile();
            this.performanceDataFile = performanceDataPath.toFile();
            this.generator = generator;
            this.numMatched = numMatched;
        }

        @Override
        public void run() {
            try (FileSegment<? extends SourceDocument> segment = collection.createFileSegment(inputFile)) {
                FileOutputStream matchesFileOutputStream = new FileOutputStream(outputMatchesFile);
                OutputStreamWriter matchesOutputStreamWriter = new OutputStreamWriter(matchesFileOutputStream);
                Writer outputMatchesWriter = new BufferedWriter(matchesOutputStreamWriter);

                FileOutputStream performanceDataFileOutputStream = new FileOutputStream(performanceDataFile);
                OutputStreamWriter performanceDataOutputStreamWriter = new OutputStreamWriter(performanceDataFileOutputStream);
                Writer performanceDataWriter = new BufferedWriter(performanceDataOutputStreamWriter);

                for (SourceDocument d : segment) {
                    Document doc = generator.createDocument(d);
                    MatchingQueries<ScoringMatch> matches = monitor.match(doc,
                        ScoringMatch.matchWithSimilarity(new BM25Similarity((float) 0.82, (float) 0.68)));
                    numMatched.incrementAndGet();

                    performanceDataWriter.write(Integer.toString(matches.getQueriesRun()));
                    performanceDataWriter.write(",");
                    performanceDataWriter.write(Integer.toString(matches.getMatchCount()));
                    performanceDataWriter.write(",");
                    performanceDataWriter.write(Long.toString(matches.getSearchTime()));
                    performanceDataWriter.write(",");
                    performanceDataWriter.write(Long.toString(matches.getQueryBuildTime()));
                    performanceDataWriter.write("\n");

                    if (matches.getMatchCount() > 0) {
                        for (ScoringMatch match : matches.getMatches()) {
                            if (d.id().equals("7067056") && match.getQueryId().equals("125705")) {
                                LOG.info("Match #125705 score: " + match.getScore());
                            }
                            if (match.getScore() > 0.45) {
                                outputMatchesWriter.write(d.id());
                                outputMatchesWriter.write(",");
                                outputMatchesWriter.write(match.getQueryId());
                                outputMatchesWriter.write("\n");
                            }
                        }
                    } else {
                        outputMatchesWriter.write(d.id());
                        outputMatchesWriter.write(",null\n");
                    }
                }
                outputMatchesWriter.close();
            } catch (Exception e) {
                LOG.error(Thread.currentThread().getName() + ": Unexpected Exception:", e.getMessage());
            }
        }
    }


}
