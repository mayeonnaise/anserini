package io.anserini.query_search;

import io.anserini.collection.DocumentCollection;
import io.anserini.collection.FileSegment;
import io.anserini.collection.SourceDocument;
import io.anserini.index.AbstractIndexer;
import io.anserini.index.generator.LuceneDocumentGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.monitor.Monitor;

import java.nio.file.Path;

public class BaseSearcher<K extends Comparable<K>> {
    private static final Logger LOG = LogManager.getLogger(BaseSearcher.class);

    protected final BaseSearchArgs args;
    protected DocumentCollection<? extends SourceDocument> collection;

    private Monitor monitor;

    public BaseSearcher(BaseSearchArgs args) {
        this.args = args;
    }

    public BaseSearcher(BaseSearchArgs args, Monitor monitor) {
        this.args = args;
        this.monitor = monitor;
    }

    public void match() {
    }

    public class MatcherThread extends Thread {
        private final Path inputFile;
        private final LuceneDocumentGenerator<SourceDocument> generator;

        public MatcherThread(Path inputFile, LuceneDocumentGenerator<SourceDocument> generator) {
            this.inputFile = inputFile;
            this.generator = generator;
        }

        @Override
        public void run() {
            try (FileSegment<? extends SourceDocument> segment = collection.createFileSegment(inputFile)) {
                for (SourceDocument d : segment) {
                    Document doc = generator.createDocument(d);

                    monitor.match(doc, );
                }
            } catch (Exception e) {
                LOG.error(Thread.currentThread().getName() + ": Unexpected Exception:", e.getMessage());
            }
        }
    }


}
