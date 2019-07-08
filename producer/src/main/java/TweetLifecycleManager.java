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

    String _consumerKey = "CONSUMER_KEY";
    String _consumerSecret = "CONSUMER_SECRET";
    String _accessToken = "TOKEN_KEY";
    String _accessTokenSecret = "TOKEN_SECRET";

    private String tracked_terms;

    public TweetLifecycleManager() {


        String terms = "futebol,bolsonaro";

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(_consumerKey)
                .setOAuthConsumerSecret(_consumerSecret)
                .setOAuthAccessToken(_accessToken)
                .setOAuthAccessTokenSecret(_accessTokenSecret);

        twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();
        listener = new TweetListener();
        twitterStream.addListener(listener);

        String tracked_terms  = terms;
        query = new FilterQuery();
        query.track(tracked_terms.split(","));
        //twitterStream.filter(query);
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
