package bigdatacourse.code;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.CqlSession;
import bigdatacourse.API;
import org.json.JSONArray;
import org.json.JSONObject;


public class cpkg implements API{

	// general consts
	public static final String		NOT_AVAILABLE_VALUE 	=		"na";
	public static final int		MAX_ROWS 	=		10000;
	public static final int maxThreads	= 25;
	// CQL stuff
	private static final String		TABLE_REVIEWS = "reviews_Office_Products";
	private static final String		TABLE_REVIEWS_BY_ITEM = "reviews_Office_Products_by_item";
	private static final String		TABLE_ITEMS = "meta_Office_Products";

	private static final String		CQL_CREATE_REVIEWS_TABLE =
			"CREATE TABLE " + TABLE_REVIEWS 	+"(" 		+
				"reviewerID text,"			+
				"asin text,"				+
				"reviewerName text,"			+
				"helpful LIST<int>,"				+
				"reviewText text,"				+
				"overall float,"				+
				"summary text,"				+
				"unixReviewTime bigint,"			+
				"reviewTime text,"				+
				"PRIMARY KEY ((reviewerID),  unixReviewTime, asin)"	+
			")   WITH CLUSTERING ORDER BY (unixReviewTime DESC, asin ASC)";

	private static final String		CQL_CREATE_REVIEWS_BY_ITEM_TABLE =
			"CREATE TABLE " + TABLE_REVIEWS_BY_ITEM 	+"(" 		+
				"reviewerID text,"			+
				"asin text,"				+
				"reviewerName text,"			+
				"helpful LIST<int>,"				+
				"reviewText text,"				+
				"overall float,"				+
				"summary text,"				+
				"unixReviewTime bigint,"			+
				"reviewTime text,"				+
				"PRIMARY KEY ((asin), unixReviewTime, reviewerID)"	+
			")  WITH CLUSTERING ORDER BY (unixReviewTime DESC, reviewerID ASC)";

	private static final String		CQL_CREATE_ITEMS_TABLE =
			"CREATE TABLE " + TABLE_ITEMS 	+"(" 		+
				"asin text,"				+
				"title text,"				+
				"price float,"				+
				"imUrl text,"				+
				"related map<text, frozen <list<text>>>,"				+
				"salesRank MAP<text,bigint>,"				+
				"brand text,"				+
				"categories SET<text>,"				+
				"description text,"				+
				"PRIMARY KEY ((asin))"	+
			") ";


	private static final String		CQL_TABLE_REVIEWS_INSERT =
			"INSERT INTO " + TABLE_REVIEWS + " (reviewerID, asin, reviewerName, helpful, reviewText, overall, summary, unixReviewTime, reviewTime) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private static final String		CQL_TABLE_REVIEWS_BY_ITEM_INSERT =
			"INSERT INTO " + TABLE_REVIEWS_BY_ITEM + " (reviewerID, asin, reviewerName, helpful, reviewText, overall, summary, unixReviewTime, reviewTime) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private static final String		CQL_TABLE_REVIEWS_SELECT =
			"SELECT * FROM " + TABLE_REVIEWS + " WHERE reviewerID = ?";

	private static final String		CQL_TABLE_REVIEWS_SELECT_BY_ITEM =
			"SELECT * FROM " + TABLE_REVIEWS_BY_ITEM + " WHERE asin = ? ";

	private static final String		CQL_TABLE_ITEMS_INSERT =
			"INSERT INTO " + TABLE_ITEMS + " (asin, title, price, imUrl, related, salesRank, brand, categories, description) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private static final String		CQL_TABLE_ITEMS_SELECT =
			"SELECT * FROM " + TABLE_ITEMS + " WHERE asin = ?";


	// cassandra session
	private CqlSession session;

	// prepared statements
	PreparedStatement reviewsAdd;
	PreparedStatement reviewsByItemAdd;
	PreparedStatement reviewsSelect;
	PreparedStatement reviewsSelectByItem;
	PreparedStatement itemAdd;
	PreparedStatement itemSelect;

	@Override
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		if (session != null) {
			System.out.println("ERROR - cassandra is already connected");
			return;
		}

		System.out.println("Initializing connection to Cassandra...");

		this.session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
				.build();

		System.out.println("Initializing connection to Cassandra... Done");
	}

	@Override
	public void close() {
		if (session == null) {
			System.out.println("Cassandra connection is already closed");
			return;
		}
		System.out.println("Closing Cassandra connection...");
		session.close();
		System.out.println("Closing Cassandra connection... Done");
	}

	@Override
	public void createTables() {
		session.execute(CQL_CREATE_ITEMS_TABLE);
		System.out.println("created table: " + TABLE_ITEMS);
		session.execute(CQL_CREATE_REVIEWS_TABLE);
		System.out.println("created table: " + TABLE_REVIEWS);
		session.execute(CQL_CREATE_REVIEWS_BY_ITEM_TABLE);
		System.out.println("created table: " + TABLE_REVIEWS_BY_ITEM);
	}

	@Override
	public void initialize() {
		reviewsAdd = session.prepare(CQL_TABLE_REVIEWS_INSERT);
		reviewsSelect = session.prepare(CQL_TABLE_REVIEWS_SELECT);
		reviewsByItemAdd = session.prepare(CQL_TABLE_REVIEWS_BY_ITEM_INSERT);
		reviewsSelectByItem = session.prepare(CQL_TABLE_REVIEWS_SELECT_BY_ITEM);
		itemAdd = session.prepare(CQL_TABLE_ITEMS_INSERT);
		itemSelect = session.prepare(CQL_TABLE_ITEMS_SELECT);
	}

	@Override
	public void loadItems(String pathItemsFile) throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
		try {
			File meta_Office_Products = new File(pathItemsFile);
			Scanner mymetaReader = new Scanner(meta_Office_Products);
			//int t=0;
			while (mymetaReader.hasNextLine()){// && t<MAX_ROWS) {
				String asin				= NOT_AVAILABLE_VALUE;
				String title			= NOT_AVAILABLE_VALUE;
				String imUrl			= NOT_AVAILABLE_VALUE;
				String brand			= NOT_AVAILABLE_VALUE;
				String description		= NOT_AVAILABLE_VALUE;
				TreeSet<String> categories = null;
				Map<String, List<String>> related = null;
				Map<String, Long> salesRank = null;
				Float price = null;
				String data = mymetaReader.nextLine();
				JSONObject json		=	new JSONObject(data);
				Set<String> s= json.keySet();

				if (s.contains("asin"))
					asin 			=	json.getString("asin");
				if (s.contains("imUrl"))
					imUrl			=	json.getString("imUrl");
				if (s.contains("brand"))
					brand			=	json.getString("brand");
				if (s.contains("description"))
					description		=	json.getString("description");
				if (s.contains("title"))
					title			=	json.getString("title");
				if (s.contains("price"))
					price   =   (float) json.getDouble("price");
				if (s.contains("categories")) {
					categories = new TreeSet<String>();
					JSONArray json1 =	json.getJSONArray("categories");
					for (int i = 0; i < json1.length(); i++) {
						JSONArray json2 =	json1.getJSONArray(i);
						for (int j = 0; j < json2.length(); j++) {
							categories.add(json2.getString(j));
						}
					}
				}
				if (s.contains("related")) {
					related 		 = new HashMap<String, List<String>>();
					JSONObject json1 =	json.getJSONObject("related");
					Set<String> s1= json1.keySet();
					for(String k: s1){
						JSONArray json2 =	json1.getJSONArray(k);
						List<String> list = new ArrayList<String>();
						for (int i = 0; i < json2.length(); i++) {
							list.add(json2.getString(i));
						}
						related.put(k, list);
					}
				}
				if(s.contains("salesRank")){
					salesRank = new HashMap<String, Long>();
					JSONObject json1 =	json.getJSONObject("salesRank");
					Set<String> s1= json1.keySet();
					for (String key : s1) {
						salesRank.put(key, json1.getLong(key));
					}
				}
				BoundStatement bstmt = itemAdd.bind(asin, title, price, imUrl, related, salesRank, brand, categories, description);
				executor.execute(new Runnable() {
					@Override
					public void run() {
						session.execute(bstmt);
					}
				});
				//t++;
			}
			mymetaReader.close();
		}
		catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}
	public void reviewsloader(String pathReviewsFile, PreparedStatement prpst)  throws Exception{
		//pathReviewsFile = "C:\\Users\\yonit\\Studies\\bigdata\\ex2\\data\\reviews_Office_Products.json";

		ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
		try{
			File reviews_Office_Products = new File(pathReviewsFile);
			Scanner myreviewReader = new Scanner(reviews_Office_Products);
			//int t=0;
			while (myreviewReader.hasNextLine()){ // && t<MAX_ROWS) {
				String asin					= NOT_AVAILABLE_VALUE;
				String reviewerID			= NOT_AVAILABLE_VALUE;
				String reviewerName			= NOT_AVAILABLE_VALUE;
				String summary				= NOT_AVAILABLE_VALUE;
				String reviewTime			= NOT_AVAILABLE_VALUE;
				String reviewText			= NOT_AVAILABLE_VALUE;
				Float overall				= null;
				ArrayList<Integer> helpful	= null;
				Long unixReviewTime			= null;
				String data = myreviewReader.nextLine();
				JSONObject json		=	new JSONObject(data);
				Set<String> s= json.keySet();

				if (s.contains("asin"))
					asin 			=	json.getString("asin");
				if (s.contains("reviewerID"))
					reviewerID		=	json.getString("reviewerID");
				if (s.contains("reviewerName"))
					reviewerName 	=	json.getString("reviewerName");
				if (s.contains("summary"))
					summary 		=	json.getString("summary");
				if (s.contains("reviewTime"))
					reviewTime 		=	json.getString("reviewTime");
				if (s.contains("reviewText"))
					reviewText 		=	json.getString("reviewText");
				if (s.contains("overall"))
					overall 		=	(float) json.getDouble("overall");
				if (s.contains("unixReviewTime"))
					unixReviewTime 	=	json.getLong("unixReviewTime");
				if (s.contains("helpful")) {
					helpful 		= new ArrayList<Integer>();
					JSONArray json1 =	json.getJSONArray("helpful");
					for (int i = 0; i < json1.length(); i++) {
						helpful.add(json1.getInt(i));
					}
				}
				BoundStatement bstmt = prpst.bind(reviewerID, asin, reviewerName, helpful, reviewText, overall, summary, unixReviewTime, reviewTime);
				executor.execute(new Runnable() {
					@Override
					public void run() {
						session.execute(bstmt);
					}
				});
				//t++;
			}
			myreviewReader.close();
		}
		catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		try{
			reviewsloader(pathReviewsFile, reviewsAdd);
			reviewsloader(pathReviewsFile, reviewsByItemAdd);
		}
		catch(Exception e){
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	@Override
	public void item(String asin) {
		BoundStatement bstmt = itemSelect.bind(asin);
		ResultSet rs = session.execute(bstmt);
		Row row = rs.one();
		if (row == null)
			System.out.println("not exists");
		while (row != null) {
			try{
				System.out.println("asin: " 		+ row.getString("asin"));
			}
			catch (NullPointerException e) {
				System.out.println("asin: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("title: " 		+ row.getString("title"));
			}
			catch (NullPointerException e) {
				System.out.println("title: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("image: " 		+ row.getString("imUrl"));
			}
			catch (NullPointerException e) {
				System.out.println("image: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("categories: " 	+ row.getSet("categories", String.class));
			}
			catch (NullPointerException e) {
				System.out.println("categories: " 	+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("description: " 	+ row.getString("description"));
			}
			catch (NullPointerException e) {
				System.out.println("description: " 	+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("price: " 		+ row.getFloat("price"));
			}
			catch (NullPointerException e) {
				System.out.println("price: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("related: " 		+ row.getMap("related", String.class, ArrayList.class));
			}
			catch (NullPointerException e) {
				System.out.println("related: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("salesRank: " 	+ row.getMap("salesRank", String.class, Long.class));
			}
			catch (NullPointerException e) {
				System.out.println("salesRank: " 	+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("brand: " 		+ row.getString("brand"));
			}
			catch (NullPointerException e) {
				System.out.println("brand: " 		+ NOT_AVAILABLE_VALUE);
			}
			row = rs.one();
		}
	}
	public int reviewshandler(BoundStatement bstmt){
		int count=0;
		ResultSet rs = session.execute(bstmt);
		Row row = rs.one();
		if (row == null)
			System.out.println("not exists");
		while (row != null) {
			try{
				System.out.println("time: " 			+ Instant.ofEpochSecond(row.getLong("unixReviewTime")));
			}
			catch (NullPointerException e) {
				System.out.println("time: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("asin: " 			+ row.getString("asin"));
			}
			catch (NullPointerException e) {
				System.out.println("asin: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("reviewerID: " 		+ row.getString("reviewerID"));
			}
			catch (NullPointerException e) {
				System.out.println("reviewerID: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("reviewerName: " 	+ row.getString("reviewerName"));
			}
			catch (NullPointerException e) {
				System.out.println("reviewerName: " 	+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("rating: " 			+ row.getFloat("overall"));
			}
			catch (NullPointerException e) {
				System.out.println("rating: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("summary: " 			+ row.getString("summary"));
			}
			catch (NullPointerException e) {
				System.out.println("summary: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("reviewText: " 		+ row.getString("reviewText"));
			}
			catch (NullPointerException e) {
				System.out.println("reviewText: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("helpful: " 			+ row.getList("helpful", Integer.class));
			}
			catch (NullPointerException e) {
				System.out.println("helpful: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("reviewTime: " 		+ row.getString("reviewTime"));
			}
			catch (NullPointerException e) {
				System.out.println("reviewTime: " 		+ NOT_AVAILABLE_VALUE);
			}
			row = rs.one();
			count++;
		}
		return count;
	}

	@Override
	public void userReviews(String reviewerID) {
		// the order of the reviews should be by the time (desc), then by the asin
		// prints the user's reviews in a descending order (latest review is printed first)
		try{
			BoundStatement bstmt = reviewsSelect.bind(reviewerID);
			System.out.println("total reviews: " + reviewshandler(bstmt));
		}
		catch (Exception e) {
			System.out.println("An error occurred.");
			e.printStackTrace();

		}
	}

	@Override
	public void itemReviews(String asin) {
		// the order of the reviews should be by the time (desc), then by the reviewerID
		// prints the items's reviews in a descending order (latest review is printed first)

		try{
			BoundStatement bstmt = reviewsSelectByItem.bind(asin);
			System.out.println("total reviews: " + reviewshandler(bstmt));
		}
		catch (Exception e) {
			System.out.println("An error occurred.");
			e.printStackTrace();

		}
	}

}
