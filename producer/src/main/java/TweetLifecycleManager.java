import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TweetLifecycleManager implements LifecycleManager, Serializable {
    private static final Logger LOGGER = Logger.getLogger(TweetLifecycleManager.class.getName());
    private final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private final TwitterStream twitterStream;
    private final FilterQuery query;
    private final TweetListener listener;

    public TweetLifecycleManager() {
        String _consumerKey = System.getenv().get("QBTNu4wt8vWDsGVYF16fInSs5");
        String _consumerSecret = System.getenv().get("Uaxc934jyI2CoSq4810HjcgHYL3sG9yYc01iStuW5rXo3v6rSr");
        String _accessToken = System.getenv().get("805917465758892036-Zj2hU6t1ecE4qyP0HjxF9X8pIlaqkvG");
        String _accessTokenSecret = System.getenv().get("1Es3vYrUfkOeGG48HFHgymU2rabqwLAnw5UUMOGoAkP0N");

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(_consumerKey)
                .setOAuthConsumerSecret(_consumerSecret)
                .setOAuthAccessToken(_accessToken)
                .setOAuthAccessTokenSecret(_accessTokenSecret);

        twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();
        listener = new TweetListener();
        twitterStream.addListener(listener);

        String tracked_terms = System.getenv().getOrDefault("TWITTER_TRACKED_TERMS", "Trump");
        query = new FilterQuery();
        query.track(tracked_terms.split(","));
    }

    public void start()  {
        if (RUNNING.compareAndSet(false, true)) {
            twitterStream.filter(query);
            LOGGER.info("Serviço iniciado");
        } else {
            LOGGER.log(Level.WARNING, "O serviço já está executando.");
        }
    }

    public void stop()  {
        if (RUNNING.compareAndSet(true, false)) {
            twitterStream.shutdown();
            listener.close();
            LOGGER.info("Serviço finalizado");
        } else {
            LOGGER.log(Level.WARNING,"O serviço não está executando. Não pode ser parado.");
        }
    }

}
