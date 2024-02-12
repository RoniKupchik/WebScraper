import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static final int maxEmails = 10;
    protected static Queue<String> linksToVisit;
    // Update the database connection details
    public static final String DB_HOST = "127.0.0.1";
    public static final String DB_NAME = "web_scraper_db";
    public static final String DB_USER = "root";
    public static final String DB_PASSWORD = System.getenv("DB_PASSWORD");

    public static void main(String[] args) {
        linksToVisit = new ConcurrentLinkedQueue<>();
        ExecutorService threadPool = Executors.newFixedThreadPool(50);
        linksToVisit.add("https://www.touro.edu");

        while (WebCrawler.emailsSize() < maxEmails) {
            if (!linksToVisit.isEmpty()) {
                synchronized (linksToVisit) {
                    String currentLink = linksToVisit.poll();
                    System.out.println(currentLink);
                    threadPool.execute(new WebCrawler(currentLink));
                    WebCrawler.linksScraped.add(currentLink);
                }
                System.out.println(WebCrawler.emailsSize() + " Total emails found");
                System.out.println(WebCrawler.linksScraped.size() + " Total links visited");
                System.out.println("----------------------");
            }
        }
        threadPool.shutdownNow();

        System.out.println("Total emails: " + WebCrawler.emailsSize());
        System.out.println("Total links visited: " + WebCrawler.linksScraped.size());
        databaseUpload();

    }

    public static void databaseUpload() {
        // Database connection URL
        String connectionUrl = "jdbc:mysql://" + DB_HOST + "/" + DB_NAME + "?user=" + DB_USER + "&password=" + DB_PASSWORD;

        // List of emails and links to upload
        List<String> emailList = new ArrayList<>(WebCrawler.emailsScraped);
        List<String> linkList = new ArrayList<>(WebCrawler.linksScraped);

        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            // Insert emails into Emails table (avoiding duplicates)
            try (PreparedStatement emailStatement = connection.prepareStatement("INSERT INTO Emails (email) SELECT ? FROM DUAL WHERE NOT EXISTS (SELECT * FROM Emails WHERE email = ?)")) {
                for (String email : emailList) {
                    // Set the email value for the PreparedStatement
                    emailStatement.setString(1, email);
                    emailStatement.setString(2, email);
                    // Add the statement to the batch
                    emailStatement.addBatch();
                }
                // Execute the batch insert
        //        emailStatement.executeBatch();
                executeBatchWithRetry(emailStatement);
            }

            // Insert links into Links table (avoiding duplicates)
            try (PreparedStatement linkStatement = connection.prepareStatement("INSERT INTO Links (url) SELECT ? FROM DUAL WHERE NOT EXISTS (SELECT * FROM Links WHERE url = ?)")) {
                for (String link : linkList) {
                    // Set the link value for the PreparedStatement
                    linkStatement.setString(1, link);
                    linkStatement.setString(2, link);
                    // Add the statement to the batch
                    linkStatement.addBatch();
                }
                // Execute the batch insert
         //       linkStatement.executeBatch();
                executeBatchWithRetry(linkStatement);
            }

            System.out.println("Data uploaded successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private static void executeBatchWithRetry(PreparedStatement statement) throws SQLException {
        boolean success = false;
        while (!success) {
            try {
                statement.executeBatch();
                success = true;
            } catch (BatchUpdateException e) {
                if (isDeadlockException(e)) {
                    System.out.println("Deadlock detected. Retrying...");
                    // Wait for a short time before retrying
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                } else {
                    throw e;
                }
            }
        }
    }
    private static boolean isDeadlockException(BatchUpdateException e) {
        Throwable cause = e.getCause();
        while (cause != null) {
            if (cause instanceof SQLTransactionRollbackException) {
                String sqlState = ((SQLTransactionRollbackException) cause).getSQLState();
                if ("40001".equals(sqlState)) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }
}