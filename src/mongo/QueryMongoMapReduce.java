package mongo;

import static com.mongodb.client.model.Projections.excludeId;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * Program to perform MapReduce queries on MongoDB.
 * Note: MapReduce support is deprecated for MongoDB version 5, but it continues
 * to work.
 */
public class QueryMongoMapReduce {
	/**
	 * MongoDB database name
	 */
	public static final String DATABASE_NAME = "lab7";

	/**
	 * MongoDB collection name
	 */
	public static final String COLLECTION_NAME = "data";

	/**
	 * Input file for data records
	 */
	private static final String INPUT_FILE = "bin/data/input.txt";

	/**
	 * MongoDB Server URL
	 */
	private static final String SERVER = "localhost";

	/**
	 * Mongo client connection to server
	 */
	private MongoClient mongoClient;

	/**
	 * Mongo database
	 */
	private MongoDatabase db;

	/**
	 * Main method
	 * 
	 * @param args
	 *             no arguments required
	 */
	public static void main(String[] args) {
		QueryMongoMapReduce qmongo = new QueryMongoMapReduce();
		qmongo.connect();
		qmongo.load();
		System.out.println(QueryMongoMapReduce.toString(qmongo.query()));
		System.out.println(QueryMongoMapReduce.toString(qmongo.query_count()));
		System.out.println(QueryMongoMapReduce.toString(qmongo.query_count_array_elements()));
		System.out.println("\nOutput query #1:");
		System.out.println(QueryMongoMapReduce.toString(qmongo.query1()));
		System.out.println("\nOutput query #2:");
		System.out.println(QueryMongoMapReduce.toString(qmongo.query2()));
		System.out.println("\nOutput query #3:");
		System.out.println(QueryMongoMapReduce.toString(qmongo.query3()));
		System.out.println("\nOutput query #4:");
		System.out.println(QueryMongoMapReduce.toString(qmongo.query4()));
		System.out.println("\nOutput query #5:");
		System.out.println(QueryMongoMapReduce.toString(qmongo.query5()));
		System.out.println("\nOutput query #6:");
		System.out.println(QueryMongoMapReduce.toString(qmongo.query6()));
	}

	/**
	 * Connects to Mongo database and returns database object to manipulate for
	 * connection.
	 * 
	 * @return
	 *         Mongo database
	 */
	public MongoDatabase connect() {
		try {
			// Provide connection information to MongoDB server
			mongoClient = MongoClients.create("mongodb://lab7:404mgbpw@" + SERVER);
		} catch (Exception ex) {
			System.out.println("Exception: " + ex);
			ex.printStackTrace();
		}

		// Provide database information to connect to
		// Note: If the database does not already exist, it will be created
		// automatically.
		db = mongoClient.getDatabase(DATABASE_NAME);
		return db;
	}

	/**
	 * Loads some sample data into MongoDB.
	 */
	public void load() {
		MongoCollection<Document> col;
		// Drop an existing collection (done to make sure we have an empty, new
		// collection each time)
		col = db.getCollection(COLLECTION_NAME);
		if (col != null)
			col.drop();

		// Create a collection
		col = db.getCollection(COLLECTION_NAME);

		// Load data from JSON documents in a file
		try {
			BufferedReader br = new BufferedReader(new FileReader(INPUT_FILE));
			String st;

			while ((st = br.readLine()) != null) {
				Object o = (DBObject) BasicDBObject.parse(st);
				@SuppressWarnings("unchecked")
				Document dbObj = new Document(((DBObject) o).toMap());
				col.insertOne(dbObj);
			}
			br.close();
		} catch (IOException e) {
			System.out.println("Working Directory = " + System.getProperty("user.dir"));
			System.out.println("Error loading data from input file: " + INPUT_FILE);
		}
	}

	/**
	 * Performs a MongoDB query that prints out all data (except for the _id).
	 */
	public MongoCursor<Document> query() {
		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);

		MongoCursor<Document> cursor = col.find().projection(excludeId()).iterator();
		return cursor;
	}

	/**
	 * Performs a MongoDB MapReduce query that counts all the documents.
	 * This will return the count of the number of customers for this particular
	 * data set.
	 */
	public MongoCursor<Document> query_count() {
		// JavaScript functions for map and reduce
		// First value ("TotalCustomers") is key, second value (1) is a value associated
		// with that key.
		String mapfn = "function() { "
				+ "emit(\"TotalCustomers\", 1); }";
		String reducefn = "function(key, items) { "
				+ "return items.length; }";

		System.out.println("\nNumber of documents (customers):");
		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
		MapReduceIterable<Document> out = col.mapReduce(mapfn, reducefn);

		// Note: This is much easier to do without using MapReduce by doing:
		// col.count();
		return out.iterator(); // Note: Make sure to return an iterator
	}

	/**
	 * Performs a MongoDB MapReduce query that counts all the array elements.
	 * This will return the total number of orders for all customers for this
	 * particular data set.
	 */
	public MongoCursor<Document> query_count_array_elements() {
		// JavaScript functions for map and reduce
		String mapfn = "function() { "
				+ "emit(\"TotalOrders\", this.orders.length); }";
		String reducefn = "function(key, items) { "
				+ " return Array.sum(items); }";

		System.out.println("\nNumber of array elements (# of orders):");
		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
		MapReduceIterable<Document> output = col.mapReduce(mapfn, reducefn);

		// Note: Using the MongoDB aggregation framework is easier than using MapReduce
		return output.iterator();
	}

	/**
	 * Performs a MongoDB MapReduce query that returns the total number of customers
	 * in each state. SELECT state, COUNT(*) GROUP BY state
	 */
	public MongoCursor<Document> query1() {
		// TODO: Write a MongoDB Map-Reduce query that returns the total number of
		// customers in each state. SELECT state, COUNT(*) GROUP BY state
		// See: https://docs.mongodb.org/manual/core/map-reduce/
		// See: https://docs.mongodb.org/manual/tutorial/map-reduce-examples/
		// See:
		// https://mongodb.github.io/mongo-java-driver/4.4/apidocs/mongodb-driver-legacy/com/mongodb/DBCollection.html#mapReduce(java.lang.String,java.lang.String,java.lang.String,com.mongodb.DBObject)

		String mapfn = "function () {"
				+ "emit(this.state, this._id); }";
		String reducefn = "function(key, items) { "
				+ "return items.length; }";

		System.out.print("\nNumber of customers in each state:");
		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);

		MapReduceIterable<Document> output = col.mapReduce(mapfn, reducefn);
		return output.iterator();
		// return null;
	}

	/**
	 * Performs a MongoDB MapReduce query that returns the total value of all
	 * orders. i.e. SUM(orders.total).
	 * Output key must be called: "totalOfAllOrders".
	 */
	public MongoCursor<Document> query2() {
		// TODO: Write a MongoDB MapReduce query that returns the total value of all
		// orders. i.e. SUM(orders.total)
		// Note: Output key must be called: "totalOfAllOrders".

		String mapfn = "function() {"
				+ "for (var i = 0; i < this.orders.length; i++) {"
				+ "var key = \"totalOfAllOrders\";"
				+ "var value = this.orders[i].total;"
				+ "emit(key, value);} };";

		String reducefn = "function(key, value) {"
				+ "reducedVal = 0;"
				+ "for (var i = 0; i < value.length; i++) {"
				+ "reducedVal += value[i];}"
				+ "return reducedVal;};";

		System.out.println("\nTotal value of all orders:");
		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
		MapReduceIterable<Document> output = col.mapReduce(mapfn, reducefn);
		return output.iterator();

		// return null;
	}

	/**
	 * Performs a MongoDB MapReduce query that returns the total value of all orders
	 * per state. SELECT state, SUM(orders.total) GROUP BY state
	 */
	public MongoCursor<Document> query3() {
		// TODO: Write a MongoDB MapReduce query that returns the total value of all
		// orders per state. SELECT state, SUM(orders.total) GROUP BY state
		String mapfn = "function() { var sum=0;"
				+ "for (var i = 0; i < this.orders.length; i++) {"
				+ "var value = this.orders[i].total;"
				+ "sum = sum + value;"
				+ "}emit(this.state, sum); };"; 

		String reducefn = "function(key, value) {"
				+ "var  reducedVal =0;"
				+ "for (var i = 0; i < value.length; i++) {"
				+ "reducedVal += value[i];}"
				+ "return reducedVal;};";

		System.out.println("\nTotal value of all orders per state:");
		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
		MapReduceIterable<Document> output = col.mapReduce(mapfn, reducefn);
		return output.iterator();

		// return null;
	}

	/**
	 * Performs a MongoDB MapReduce query that returns the average # of items per
	 * order by state with name > 'K'. SELECT state,
	 * COUNT(orders.items)/COUNT(orders) WHERE name > 'K' GROUP BY state
	 */
	public MongoCursor<Document> query4() {
		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
		String mapfn = "function() { "
			+ "var qty = 0; "
			+ "var count = 0; "
			+ "var name = this.name;"
			+ "if (name > 'K') { "
			+ "for (var i = 0; i < this.orders.length; i++) { "
			+ "qty += this.orders[i].items; "
			+ "count++; }"
			+ "emit(this.state, {qty: qty, count: count}); }}";
		String reducefn = "function(key, value) { "
			+ "var reducedVal = {qty: 0, count: 0}; "
			+ "var count = 0; "
			+ "for (var i = 0; i < value.length; i++) { "
			+ "reducedVal.qty += value[i].qty; "
			+ "reducedVal.count += value[i].count; }"
			+ "return reducedVal; }";
		String finalizefn = "function(key, value) { "
			+ "return value.count === 0 ? 0.0 : value.qty / value.count; }";
		MapReduceIterable<Document> output = col.mapReduce(mapfn, reducefn).finalizeFunction(finalizefn);
		return output.iterator();
	}

		// return null;



	/**
     * Performs a MongoDB MapReduce query that find the order with the maximum # of items. SELECT MAX(orders.item) 
     */
    public MongoCursor<Document> query5()
    {
    	// TODO: Write a MongoDB MapReduce query that find the order with the maximum # of items. SELECT MAX(orders.item) 
    	// Note: Output key should be "max".
    	String mapfn = "function() {"
		
			+"for (var i = 0; i < this.orders.length; i++) {"
			+"var key = \"max\";"
			+"var max = this.orders[0].items;"
			+"var num = 0;"
			+"var items = 0;"
			+"var total = 0;"
			+"if (this.orders[i].items > max) {" //cant get the correct max index 
				+"max = this.orders[i].items;" 
				+"items += max;"
				+"num += this.orders[i].num;"
				+"total += this.orders[i].total; }"
			+"emit(key, {num, items, total}); }};";

		String reducefn = "function(key, value) {"
				+ "reducedVal = { num: 0, items: 0, total: 0 };"
				+ "for (var i = 0; i < value.length; i++) {"
				+ "reducedVal.num += value[i].num;"
				+ "reducedVal.items += value[i].items;"
				+ "reducedVal.total += value[i].total; }"
				+ "return reducedVal;};";

		System.out.println("\nMaximum # of items:");
		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
		MapReduceIterable<Document> output = col.mapReduce(mapfn, reducefn);
		return output.iterator();

		// return null;
	}

	/**
	 * Performs a MongoDB MapReduce query that determines the number of times first
	 * name is: longer than last name, same as last name, and shorter than last
	 * name.
	 */
	public MongoCursor<Document> query6() {
		// TODO: Write a MongoDB MapReduce query that determines the number of times
		// first name is: longer than last name, same as last name, and shorter than
		// last name.
		// Note: Output keys should be "firstlonger", "firstshorter", and "same".

		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);

		return null;
	}

	/**
	 * Returns the Mongo database being used.
	 * 
	 * @return
	 *         Mongo database
	 */
	public MongoDatabase getDb() {
		return db;
	}

	/**
	 * Outputs a cursor of MongoDB results in string form.
	 * 
	 * @param cursor
	 *               Mongo cursor
	 * @return
	 *         results as a string
	 */
	public static String toString(MongoCursor<Document> cursor) {
		StringBuilder buf = new StringBuilder();
		int count = 0;
		buf.append("Rows:\n");
		if (cursor != null) {
			while (cursor.hasNext()) {
				Document obj = cursor.next();
				buf.append(obj.toJson());
				buf.append("\n");
				count++;
			}
			cursor.close();
		}
		buf.append("Number of rows: " + count);
		return buf.toString();
	}
}
