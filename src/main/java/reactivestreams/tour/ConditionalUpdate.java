package reactivestreams.tour;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import reactivestreams.helpers.SubscriberHelpers.ObservableSubscriber;
import reactivestreams.helpers.SubscriberHelpers.OperationSubscriber;
import reactivestreams.helpers.SubscriberHelpers.PrintDocumentSubscriber;
import reactivestreams.helpers.SubscriberHelpers.PrintSubscriber;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * The QuickTour code example
 */
public class ConditionalUpdate {
    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes an optional single argument for the connection string
     */
    public static void main(final String[] args) {
        MongoClient mongoClient;

        if (args.length == 0) {
            // connect to the local database server
            mongoClient = MongoClients.create();
        } else {
            mongoClient = MongoClients.create(args[0]);
        }

        // get handle to "mydb" database
        MongoDatabase database = mongoClient.getDatabase("mydb");

        // get a handle to the "test" collection
        final MongoCollection<Document> collection = database.getCollection("test");

        // drop all the data in it
        ObservableSubscriber<Void> successSubscriber = new OperationSubscriber<>();
        collection.drop().subscribe(successSubscriber);
        successSubscriber.await();

        // make a document and insert it
        System.out.println("\n*** Inserting test record ***");
        Document doc = new Document("_id", 1)
                .append("item_name", "Rise of the Resistence")
                .append("seats", 5);

        ObservableSubscriber<InsertOneResult> insertOneSubscriber = new OperationSubscriber<>();
        collection.insertOne(doc).subscribe(insertOneSubscriber);
        insertOneSubscriber.await();

        // get it (since it's the only one in there since we dropped the rest earlier
        // on)
        System.out.println("\n*** Inserted document ***");
        ObservableSubscriber<Document> documentSubscriber = new PrintDocumentSubscriber();
        collection.find().first().subscribe(documentSubscriber);
        documentSubscriber.await();

        // Aggregation
        // System.out.println(
        //         "\n*** Conditional aggregation using PrintDocumentSubscriber<>  ***");
        // documentSubscriber = new PrintDocumentSubscriber();
        // collection.aggregate(asList(
        //         set("seats", Document.parse(
        //                 "{ $cond: { 'if': { $gte: [ '$seats', 4 ] }, then: { $subtract: [ '$seats', 4 ] }, 'else': '$seats' } } } "))))
        //         .subscribe(documentSubscriber);
        // documentSubscriber.await();

        // Conditional Update
        System.out.println("\n*** Conditional update, removing 5 seats ***");
        ObservableSubscriber<UpdateResult> updateSubscriber = new PrintSubscriber<>("Update Result: %s");
        collection.updateOne(eq("_id", 1), asList(
                set("seats", Document.parse(
                        "{ $cond: { 'if': { $gte: [ '$seats', 4 ] }, then: { $subtract: [ '$seats', 4 ] }, 'else': '$seats' } } } "))))
                .subscribe(updateSubscriber);
        updateSubscriber.await();

        // get it (since it's the only one in there since we dropped the rest earlier
        // on)
        System.out.println("\n*** Record after update  ***");
        documentSubscriber = new PrintDocumentSubscriber();
        collection.find().first().subscribe(documentSubscriber);
        documentSubscriber.await();

        // Conditional Update
        System.out.println("\n*** Conditional update, removing 5 seats ***");
        updateSubscriber = new PrintSubscriber<>("Update Result: %s");
        collection.updateOne(eq("_id", 1), asList(
                set("seats", Document.parse(
                        "{ $cond: { 'if': { $gte: [ '$seats', 4 ] }, then: { $subtract: [ '$seats', 4 ] }, 'else': '$seats' } } } "))))
                .subscribe(updateSubscriber);
        updateSubscriber.await();

        // get it (since it's the only one in there since we dropped the rest earlier
        // on)
        System.out.println("\n*** Record after update  ***");
        documentSubscriber = new PrintDocumentSubscriber();
        collection.find().first().subscribe(documentSubscriber);
        documentSubscriber.await();        

        // Clean up
        System.out.println("\n*** Clean up: drop()  ***");
        successSubscriber = new OperationSubscriber<>();
        collection.drop().subscribe(successSubscriber);
        successSubscriber.await();

        // release resources
        mongoClient.close();
    }
}