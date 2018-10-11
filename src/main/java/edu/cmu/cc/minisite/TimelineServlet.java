package edu.cmu.cc.minisite;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import java.io.IOException;
import java.io.PrintWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.bson.Document;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

/**
 * Task 4 (1):
 * Get the name and profile of the user as you did in Task 1
 * Put them as fields in the result JSON object
 *
 * Task 4 (2);
 * Get the follower name and profiles as you did in Task 2
 * Put them in the result JSON object as one array
 *
 * Task 4 (3):
 * From the user's followees, get the 30 most popular comments
 * and put them in the result JSON object as one JSON array.
 * (Remember to find their parent and grandparent)
 *
 * The posts should be sorted:
 * First by ups in descending order.
 * Break tie by the timestamp in descending order.
 */
public class TimelineServlet extends HttpServlet {

    /**
     * MySQL database.
     */
    private static Connection conn;
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_DB_NAME = "reddit_db";
    private static String MYSQL_HOST = System.getenv("MYSQL_HOST");
    private static String MYSQL_NAME = System.getenv("MYSQL_NAME");
    private static String MYSQL_PWD = System.getenv("MYSQL_PWD");
    private static final String MYSQL_URL = "jdbc:mysql://" + MYSQL_HOST + ":3306/"
            + MYSQL_DB_NAME + "?useSSL=false";

    /**
     * Neo4j database.
     */
    private final Driver driver;
    private static String NEO4J_HOST = System.getenv("NEO4J_HOST");
    private static String NEO4J_NAME = System.getenv("NEO4J_NAME");
    private static String NEO4J_PWD = System.getenv("NEO4J_PWD");
    private static final String NEO4J_URL = "bolt://" + NEO4J_HOST + ":7687";

    /**
     * MongoDB server.
     */
    private static MongoCollection<Document> collection;
    private static String MONGO_HOST = System.getenv("MONGO_HOST");
    private static final String MONGO_URL = "mongodb://" + MONGO_HOST + ":27017";
    private static final String MONGO_DB_NAME = "reddit_db";
    private static final String COLLECTION_NAME = "posts";

    /**
     * Your initialization code goes here.
     */
    public TimelineServlet() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        Objects.requireNonNull(MYSQL_HOST);
        Objects.requireNonNull(MYSQL_NAME);
        Objects.requireNonNull(MYSQL_PWD);
        conn = DriverManager.getConnection(MYSQL_URL, MYSQL_NAME, MYSQL_PWD);

        Objects.requireNonNull(NEO4J_HOST);
        Objects.requireNonNull(NEO4J_NAME);
        Objects.requireNonNull(NEO4J_PWD);
        driver = GraphDatabase.driver(NEO4J_URL, AuthTokens.basic(NEO4J_NAME, NEO4J_PWD));

        Objects.requireNonNull(MONGO_HOST);
        MongoClientURI connectionString = new MongoClientURI(MONGO_URL);
        MongoClient mongoClient = new MongoClient(connectionString);
        MongoDatabase database = mongoClient.getDatabase(MONGO_DB_NAME);
        collection = database.getCollection(COLLECTION_NAME);
    }

    /**
     * Implement this method.
     *
     * @param request  the request object that is passed to the servlet
     * @param response the response object that the servlet
     *                 uses to return the headers to the client
     * @throws IOException      if an input or output error occurs
     * @throws ServletException if the request for the HEAD
     *                          could not be handled
     */
    @Override
    protected void doGet(final HttpServletRequest request,
                         final HttpServletResponse response) throws ServletException, IOException {

        JsonObject result = new JsonObject();
        String id = request.getParameter("id");

        /**
         * Access MySQL database to get profile.
         */
        String profileUrl = "";
        try {
            PreparedStatement stmt = conn.prepareStatement(
                "SELECT * FROM users WHERE username = ?");
            stmt.setString(1, id);

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                profileUrl = rs.getString("profile_photo_url");
                break;
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e);
        }
        result.addProperty("name", id);
        result.addProperty("profile", profileUrl);

        /**
         * Access Neo4j database to get followers information.
         */
        String query = "MATCH (follower:User)-[r:FOLLOWS]->(followee:User)"
                        + " WHERE followee.username = '" + id
                        + "' RETURN follower.username, follower.url"
                        + " ORDER BY follower.username ASC;";
        JsonArray followers = new JsonArray();
        try (Session session = driver.session()) {
            StatementResult rs = session.run(query);
            while (rs.hasNext()) {
                Record record = rs.next();
                JsonObject follower = new JsonObject();
                follower.addProperty("profile", record.get(1).asString());
                follower.addProperty("name", record.get(0).asString());
                followers.add(follower);
            }
        }
        result.add("followers", followers);

        /**
         * Access Neo4j database to get followees information.
         */
        Set<String> followees = new HashSet<>();
        query = "MATCH (follower:User)-[r:FOLLOWS]->(followee:User)"
                + " WHERE follower.username = '" + id
                + "' RETURN followee.username";
        try (Session session = driver.session()) {
            StatementResult rs = session.run(query);
            while (rs.hasNext()) {
                Record record = rs.next();
                followees.add(record.get(0).asString());
            }
        }

        /**
         * Access MongoDB to get comments.
         */
        MongoCursor<Document> cursor = collection.find(Filters.in("uid", followees))
                .sort(Sorts.orderBy(Sorts.descending("ups"), Sorts.descending("timestamp")))
                .projection(Projections.fields(Projections.exclude(Arrays.asList("_id"))))
                .limit(30).iterator();

        JsonArray comments = new JsonArray();
        try {
            while (cursor.hasNext()) {
                JsonObject comment = new JsonParser().parse(cursor.next().toJson())
                        .getAsJsonObject();
                String parentId = comment.get("parent_id").getAsString();
                MongoCursor<Document> parentCursor = collection.find(Filters.eq("cid", parentId))
                        .projection(Projections.fields(Projections.exclude(Arrays.asList("_id"))))
                        .iterator();
                try {
                    while (parentCursor.hasNext()) {
                        JsonObject parentComment = new JsonParser()
                                .parse(parentCursor.next().toJson())
                                .getAsJsonObject();
                        comment.add("parent", parentComment);
                        String grandparentId = parentComment.get("parent_id").getAsString();
                        MongoCursor<Document> grandparentCursor = collection
                            .find(Filters.eq("cid", grandparentId))
                            .projection(Projections.fields(
                                Projections.exclude(Arrays.asList("_id"))))
                            .iterator();
                        try {
                            while (grandparentCursor.hasNext()) {
                                JsonObject grandparentComment = new JsonParser()
                                    .parse(grandparentCursor.next().toJson())
                                    .getAsJsonObject();
                                comment.add("grand_parent", grandparentComment);
                            }
                        } finally {
                            grandparentCursor.close();
                        }
                    }
                } finally {
                    parentCursor.close();
                }
                comments.add(comment);
            }
        } finally {
            cursor.close();
        }
        result.add("comments", comments);

        response.setContentType("text/html; charset=UTF-8");
        response.setCharacterEncoding("UTF-8");
        PrintWriter writer = response.getWriter();
        writer.print(result.toString());
        writer.close();
    }
}

