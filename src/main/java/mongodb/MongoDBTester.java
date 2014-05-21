package mongodb;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.Set;

/**
 * Created by teots on 3/13/14.
 */
public class MongoDBTester {
    public static void main(String[] args) {
        MongoClient mongoClient = null;
        try {
            // Connect to mongo server.
            mongoClient = new MongoClient("localhost");

            // Connect to DB.
            DB db = mongoClient.getDB("test");
            System.out.println("Connect to database successfully");

            // Get all collections in this database.
            System.out.println("Collections:");
            Set<String> colls = db.getCollectionNames();
            for (String s : colls) {
                System.out.println("\t" + s);
            }

            // Create/get a collection.
            DBCollection coll = null;
            if (db.collectionExists("mycol")) {
                coll = db.getCollection("mycol");
                coll.drop();
            }
            DBObject options = BasicDBObjectBuilder.start().add("capped", false).get();
            coll = db.createCollection("mycol", options);
            System.out.println("Collection created/get successfully");

            // Insert a document into the selected collection.
            BasicDBObject doc = new BasicDBObject("title", "MongoDB").
                    append("description", "database").
                    append("likes", 100).
                    append("url", "http://www.tutorialspoint.com/mongodb/").
                    append("by", "tutorials point").
                    append("info", new BasicDBObject("x", 203).append("y", 102));
            coll.insert(doc);
            System.out.println("Document inserted successfully");

            // Read the newly added document.
            DBObject myDoc = coll.findOne();
            System.out.println(myDoc);

            // Get the number of documents in the collection.
            System.out.println("# of documents: " + Long.toString(coll.getCount()));

            // Retrieve all documents in the collection.
            DBCursor cursor = coll.find();
            int i = 1;
            while (cursor.hasNext()) {
                System.out.println("Inserted Document: " + i);
                System.out.println(cursor.next());
                i++;
            }

            // Update
            BasicDBObject query = new BasicDBObject("likes", 100);
            BasicDBObject documentUpdate = new BasicDBObject("$set", new BasicDBObject("likes", 200));
            coll.update(query, documentUpdate, true, true);

            // Get all updated documents.
            query = new BasicDBObject("likes", 200);
            cursor = coll.find(query);
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }

            // Delete the first document.
            DBObject firstDoc = coll.findOne();
            coll.remove(firstDoc);

            // Get the number of documents in the collection.
            System.out.println("# of documents: " + Long.toString(coll.getCount()));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }
}
