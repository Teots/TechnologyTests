package lucene;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import java.io.*;
import java.util.Date;

/**
 * Created by teots on 3/13/14.
 */
public class LuceneTester {
    public static void main(String[] args) {
        try {
            // Create an index.
            StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);

            Directory indexDirectory = new RAMDirectory();
            //Directory indexDirectory = FSDirectory.open(new File("lucDirHello") );

            // Create a new index. (old indices are removed)
            IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            iwc.setRAMBufferSizeMB(256.0d);
            IndexWriter iw = new IndexWriter(indexDirectory, iwc);

            File folder = new File("/home/teots/Desktop/text");
            long start = System.currentTimeMillis();
            indexDocs(iw, folder);
            iw.close();
            System.out.println(System.currentTimeMillis() - start + " total milliseconds");

            // Query the previously created index.
            IndexReader reader = DirectoryReader.open(indexDirectory);
            IndexSearcher searcher = new IndexSearcher(reader);

            String queryString = null;
            String field = "contents";
            QueryParser parser = new QueryParser(Version.LUCENE_47, field, analyzer);

            BufferedReader in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
            while (true) {
                if (queryString == null) {                        // prompt the user
                    System.out.println("Enter query: ");
                }

                String line = queryString != null ? queryString : in.readLine();

                if (line == null || line.length() == -1) {
                    break;
                }

                line = line.trim();
                if (line.length() == 0) {
                    break;
                }

                Query query = parser.parse(line);
                System.out.println("Searching for: " + query.toString(field));

                // repeat & time as benchmark
                Date s = new Date();
                TopDocs results = searcher.search(query, null, 100);
                Date end = new Date();
                System.out.println("Time: " + (end.getTime() - s.getTime()) + "ms");

                ScoreDoc[] hits = results.scoreDocs;
                System.out.println(results.totalHits + " total matching documents");
                for (int i = 0; i < hits.length; ++i) {
                    System.out.println(hits[i]);
                    for (IndexableField indexableField : searcher.doc(hits[i].doc).getFields()) {
                        System.out.println("\t" + indexableField);
                    }
                }

                if (queryString != null) {
                    break;
                }
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    static void indexDocs(IndexWriter writer, File file) throws IOException {
        // do not try to index files that cannot be read
        if (file.canRead()) {
            if (file.isDirectory()) {
                String[] files = file.list();
                if (files != null) {
                    for (int i = 0; i < files.length; i++) {
                        indexDocs(writer, new File(file, files[i]));
                    }
                }
            } else {
                FileInputStream fis;
                try {
                    fis = new FileInputStream(file);
                } catch (FileNotFoundException fnfe) {
                    // at least on windows, some temporary files raise this exception with an "access denied" message
                    // checking if the file can be read doesn't help
                    return;
                }

                try {
                    // make a new, empty document
                    Document doc = new Document();

                    // Add the path of the file as a field named "path".  Use a
                    // field that is indexed (i.e. searchable), but don't tokenize
                    // the field into separate words and don't index term frequency
                    // or positional information:
                    Field pathField = new StringField("path", file.getPath(), Field.Store.YES);
                    doc.add(pathField);

                    // Add the last modified date of the file a field named "modified".
                    // Use a LongField that is indexed (i.e. efficiently filterable with
                    // NumericRangeFilter).  This indexes to milli-second resolution, which
                    // is often too fine.  You could instead create a number based on
                    // year/month/day/hour/minutes/seconds, down the resolution you require.
                    // For example the long value 2011021714 would mean
                    // February 17, 2011, 2-3 PM.
                    doc.add(new LongField("modified", file.lastModified(), Field.Store.NO));

                    // Add the contents of the file to a field named "contents".  Specify a Reader,
                    // so that the text of the file is tokenized and indexed, but not stored.
                    // Note that FileReader expects the file to be in UTF-8 encoding.
                    // If that's not the case searching for special characters will fail.
                    doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(fis, "UTF-8"))));

                    if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
                        // New index, so we just add the document (no old document can be there):
                        System.out.println("adding " + file);
                        writer.addDocument(doc);
                    } else {
                        // Existing index (an old copy of this document may have been indexed) so
                        // we use updateDocument instead to replace the old one matching the exact
                        // path, if present:
                        System.out.println("updating " + file);
                        writer.updateDocument(new Term("path", file.getPath()), doc);
                    }
                } finally {
                    fis.close();
                }
            }
        }
    }
}
