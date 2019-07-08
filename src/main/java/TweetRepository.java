import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.ArrayList;
import java.util.List;
import com.datastax.driver.core.LocalDate;
import twitter4j.GeoLocation;


public class TweetRepository {

    private static final String TABLE_NAME = "tweets";

    private static final String TABLE_NAME_BY_USER = TABLE_NAME + "ByUsername";

    private Session session;

    public TweetRepository(Session session)
    {
        this.session = session;
    }

    public void createTable()
    {

        System.out.println("createTable - init");

        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME).append("(")
                .append("tweetId int PRIMARY KEY,")
                .append("tweetDate date,")
                .append("tweetUsername text,")
                .append("tweetText text,")
                .append("tweetSource text,")
                .append("isTweetTruncated boolean,")
                .append("isTweetFavorited boolean,")
                .append("tweetGeoLat double,")
                .append("tweetGeoLong double")
                .append(");");

        final String query = sb.toString();
        session.execute(query);

        System.out.println("createTable - command: " + sb);

        System.out.println("createTable - end");

    }

    public void createTableTweetsByUsername()
    {

        System.out.println("createTableTweetsByUsername - init");

        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME_BY_USER)
                .append("(").append("tweetId int, ").append("tweetDate date, ").append("tweetUsername text, ").append("tweetText text,")
                .append("tweetSource text,")
                .append("isTweetTruncated boolean,")
                .append("isTweetFavorited boolean,")
                .append("tweetGeoLat double,")
                .append("tweetGeoLong double,")
                .append("PRIMARY KEY (tweetUsername, tweetId));");

        final String query = sb.toString();
        session.execute(query);
        System.out.println("createTableTweetsByUsername - command: " + sb);

        System.out.println("createTableTweetsByUsername - end");


    }

    public void insertTweet(Tweet tw)
    {
        System.out.println("insertTweet - init");
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME).append("(tweetId, tweetDate, tweetUsername, tweetText, tweetSource, isTweetTruncated, isTweetFavorited, tweetGeoLat, tweetGeoLong) ")
                .append("VALUES (").append(tw.getId()).append(", '")
                .append(tw.getTweetDate()).append("', '")
                .append(tw.getUsername()).append("', '")
                .append(tw.getTweetText()).append("', '")
                .append(tw.getSource()).append("', ")
                .append(tw.isTruncated()).append(", ")
                .append(tw.isFavorited()).append(", ")
                .append(tw.getGeoLocation().getLatitude()).append(", ")
                .append(tw.getGeoLocation().getLongitude())
                .append(");");

        final String query = sb.toString();
        System.out.println("insertTweet - command: " + sb);
        session.execute(query);
        System.out.println("insertTweet - end");
    }

    public void insertTweetByUsername(Tweet tw)
    {
        System.out.println("insertTweetByUsername - init");
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME_BY_USER).append("(tweetId, tweetDate, tweetUsername, tweetText, tweetSource, isTweetTruncated, isTweetFavorited, tweetGeoLat, tweetGeoLong) ")
                .append("VALUES (").append(tw.getId()).append(", '")
                .append(tw.getTweetDate()).append("', '")
                .append(tw.getUsername()).append("', '")
                .append(tw.getTweetText()).append("', '")
                .append(tw.getSource()).append("', ")
                .append(tw.isTruncated()).append(", ")
                .append(tw.isFavorited()).append(", ")
                .append(tw.getGeoLocation().getLatitude()).append(", ")
                .append(tw.getGeoLocation().getLongitude())
                .append(");");

        final String query = sb.toString();
        System.out.println("insertTweetByUsername - command: " + sb);
        session.execute(query);
        System.out.println("insertTweetByUsername - end");
    }

    public Tweet selectByUsername(String user)
    {
        System.out.println("selectTweetByUsername - init");
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(TABLE_NAME_BY_USER)
                .append(" WHERE tweetUsername = '").append(user).append("' ")
                .append("ALLOW FILTERING;");

        final String query = sb.toString();

        System.out.println("selectTweetByUsername - command: " + sb);

        ResultSet rs = session.execute(query);

       // System.out.println("selectTweetByUsername - execute command");

        List<Tweet> tweets = new ArrayList<Tweet>();

      //  System.out.println("selectTweetByUsername - create list");

        for (Row r : rs)
        {
            Tweet s = new Tweet((long)r.getInt("tweetId"),
                    r.getDate("tweetDate"),
                    r.getString("tweetUsername"),
                    r.getString("tweetText"),
                    r.getDouble("tweetGeoLat"),
                    r.getDouble("tweetGeoLong"),
                    r.getString("tweetSource"),
                    r.getBool("isTweetTruncated"),
                    r.getBool("isTweetFavorited")
                    );
            //System.out.println("selectTweetByUsername - create tweet");

            tweets.add(s);
            //System.out.println("selectTweetByUsername - add tweet to list");
        }

        Tweet twtRef = tweets.get(0);

        System.out.println("TweetSelectByUsername = " + twtRef.getId() + ", "
                + twtRef.getTweetDate() + ", "
                + twtRef.getUsername() + ", "
                + twtRef.getTweetText() + ", "
                + twtRef.getSource() + ", "
                + twtRef.isTruncated() + ", "
                + twtRef.isFavorited() + ", "
                + twtRef.getGeoLocation().getLatitude() + ", "
                + twtRef.getGeoLocation().getLongitude());

        System.out.println("selectTweetByUsername - end");
        return twtRef;
    }

    public List<Tweet> selectAll() {
        System.out.println("selectAllTweets - init");
        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_NAME);

        final String query = sb.toString();
        ResultSet rs = session.execute(query);

        List<Tweet> tweets = new ArrayList<Tweet>();

        for (Row r : rs) {
            Tweet twt = new Tweet((long)r.getInt("tweetId"),
                    r.getDate("tweetDate"),
                    r.getString("tweetUsername"),
                    r.getString("tweetText"),
                    r.getDouble("tweetGeoLat"),
                    r.getDouble("tweetGeoLong"),
                    r.getString("tweetSource"),
                    r.getBool("isTweetTruncated"),
                    r.getBool("isTweetFavorited")
            );
            System.out.println("Tweet = " + twt.getId() + ", "
                    + twt.getTweetDate() + ", "
                    + twt.getUsername() + ", "
                    + twt.getTweetText() + ", "
                    + twt.getSource() + ", "
                    + twt.isTruncated() + ", "
                    + twt.isFavorited() + ", "
                    + twt.getGeoLocation().getLatitude() + ", "
                    + twt.getGeoLocation().getLongitude());

            tweets.add(twt);
        }

        System.out.println("selectAllTweets - end");
        return tweets;
    }

    public List<Tweet> selectAllByUsername() {
        System.out.println("selectAllTableByUsername - init");

        StringBuilder sb = new StringBuilder("SELECT * FROM ").append(TABLE_NAME_BY_USER);

        final String query = sb.toString();
        ResultSet rs = session.execute(query);

        List<Tweet> tweets = new ArrayList<Tweet>();

        for (Row r : rs) {
            Tweet twt = new Tweet((long)r.getInt("tweetId"),
                    r.getDate("tweetDate"),
                    r.getString("tweetUsername"),
                    r.getString("tweetText"),
                    r.getDouble("tweetGeoLat"),
                    r.getDouble("tweetGeoLong"),
                    r.getString("tweetSource"),
                    r.getBool("isTweetTruncated"),
                    r.getBool("isTweetFavorited")
            );
            System.out.println("TweetByUsername = " + twt.getId() + ", "
                    + twt.getTweetDate() + ", "
                    + twt.getUsername() + ", "
                    + twt.getTweetText() + ", "
                    + twt.getSource() + ", "
                    + twt.isTruncated() + ", "
                    + twt.isFavorited() + ", "
                    + twt.getGeoLocation().getLatitude() + ", "
                    + twt.getGeoLocation().getLongitude());

            tweets.add(twt);
        }

        System.out.println("selectAllTableByUsername - end");
        return tweets;
    }

    public void deleteTweetById(long id)
    {
        System.out.println("deleteTweetById - init");

        StringBuilder sb = new StringBuilder("DELETE FROM ")
                .append(TABLE_NAME)
                .append(" WHERE tweetId = ")
                .append(String.valueOf(id)).append(";");

        final String query = sb.toString();

        System.out.println("deleteTweetById - exec command " + query);
        session.execute(query);

        System.out.println("deleteTweetById - end");
    }

    public void deleteTweetByUsername(String user)
    {
        Tweet twtRef = selectByUsername(user);

        System.out.println("deleteTweetByUsername - init");
        StringBuilder sb = new StringBuilder("DELETE FROM ")
                .append(TABLE_NAME_BY_USER)
                .append(" WHERE tweetUsername = '")
                .append(twtRef.getUsername()).append("';");

        final String query = sb.toString();
        System.out.println("deleteTweetByUsername - exec command " + query);
        session.execute(query);

        System.out.println("deleteTweetByUsername - end");
    }

    public void deleteTable(String tableName)
    {
        System.out.println("deleteTable - init");

        StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ").append(tableName);

        final String query = sb.toString();

        System.out.println("deleteTable - exec command " + query);
        session.execute(query);

        System.out.println("deleteTable - end");
    }
}
