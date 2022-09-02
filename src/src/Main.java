import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static final int maxEmails = 10000;
    protected static Queue<String> linksToVisit;

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
        //never show credentials
        String connectionUrl =
                "jdbc:sqlserver://database-1.cbjmpwcdjfmq.us-east-1.rds.amazonaws.com;"
                        + "database=Kupchik_Roni;"
                        + "user="
                        + "password="
                        + "encrypt=false;"
                        + "trustServerCertificate=false;"
                        + "loginTimeout=30;";
        List<String> list = new ArrayList<String>(WebCrawler.emailsScraped);
        for (int i = 0; i < WebCrawler.emailsSize(); i++) {
            try (Connection connection = DriverManager.getConnection(connectionUrl);
                 PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO Emails VALUES (?)")) {
                preparedStatement.setString(1, list.get(i));
                preparedStatement.execute();

            } catch (SQLException throwables) {
                throwables.printStackTrace();

            }
        }
    }
}