package edu.cmu.cc.minisite;

import com.google.gson.JsonObject;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * Task 1:
 * This query simulates the login process of a user
 * and tests whether your backend system is functioning properly.
 * Your web application will receive a pair of UserName and Password,
 * and you need to check your backend database to see if the
 * UserName and Password is a valid pair.
 * You should construct your response accordingly:
 *
 * If YES, send back the userName and Profile Image URL.
 * If NOT, set userName as "Unauthorized" and Profile Image URL as "#".
 */
public class ProfileServlet extends HttpServlet {

    /**
     * JDBC driver of MySQL Connector/J.
     */
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    /**
     * Database name.
     */
    private static final String DB_NAME = "reddit_db";

    /**
     * The endpoint of the database.
     *
     * To avoid hardcoding credentials, use environment variables to include
     * the credentials.
     *
     * e.g., before running "mvn clean package exec:java" to start the server
     * run the following commands to set the environment variables.
     * export MYSQL_HOST=...
     * export MYSQL_NAME=...
     * export MYSQL_PWD=...
     */
    private static String MYSQL_HOST = System.getenv("MYSQL_HOST");
    /**
     * MySQL username.
     */
    private static String MYSQL_NAME = System.getenv("MYSQL_NAME");
    /**
     * MySQL Password.
     */
    private static String MYSQL_PWD = System.getenv("MYSQL_PWD");

    /**
     * The connection (session) with the database.
     */
    private static Connection conn;

    /**
     * MySQL URL.
     */
    private static final String URL = "jdbc:mysql://" + MYSQL_HOST + ":3306/"
            + DB_NAME + "?useSSL=false";

    /**
     * Initialize SQL connection.
     *
     * @throws ClassNotFoundException when an application fails to load a class
     * @throws SQLException           on a database access error or other errors
     */
    public ProfileServlet() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        Objects.requireNonNull(MYSQL_HOST);
        Objects.requireNonNull(MYSQL_NAME);
        Objects.requireNonNull(MYSQL_PWD);
        conn = DriverManager.getConnection(URL, MYSQL_NAME, MYSQL_PWD);
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
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        JsonObject result = new JsonObject();
        String name = request.getParameter("id");
        String pwd = request.getParameter("pwd");

        boolean foundIdentity = false;
        String profileUrl = "#";
        try {
            PreparedStatement stmt = conn.prepareStatement(
                "SELECT * FROM users WHERE username = ? AND pwd = ?");
            stmt.setString(1, name);
            stmt.setString(2, pwd);

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                foundIdentity = true;
                profileUrl = rs.getString("profile_photo_url");
                break;
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e);
        }

        if (!foundIdentity) {
            name = "Unauthorized";
        }
        result.addProperty("name", name);
        result.addProperty("profile", profileUrl);
        response.setContentType("text/html; charset=UTF-8");
        response.setCharacterEncoding("UTF-8");
        PrintWriter writer = response.getWriter();
        writer.write(result.toString());
        writer.close();
    }
}
