import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebCrawler implements Runnable {

    //no duplicates allowed in a set
    public static Set<String> linksScraped = Collections.synchronizedSet(new HashSet<>());
    public static Set<String> emailsScraped = Collections.synchronizedSet(new HashSet<>());
    private String url;

    public WebCrawler(String url) {
        this.url = url;
    }

    public static int emailsSize(){
        return emailsScraped.size();
    }
    public static int linksSize(){
        return linksScraped.size();
    }
    @Override
    public void run() {
        try {
            Document webSite = Jsoup.connect(url).userAgent("Chrome").get();

            Elements elements = webSite.select("a[href]");
            for (Element e : elements) {
                linksScraped.add(e.attr("abs:href"));
            }

            Main.linksToVisit.addAll(linksScraped);
            Pattern p = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+");
            Matcher matcher = p.matcher(webSite.text());
            while(matcher.find()) {
                String email = matcher.group().toLowerCase();
                emailsScraped.add(email);
                //add to database in batches, at every 100 emails
                if(emailsScraped.size() >= 100){
                    Main.databaseUpload();
                }
            }

        }
        catch(IOException e) {
//			e.printStackTrace();
        }
    }
}
