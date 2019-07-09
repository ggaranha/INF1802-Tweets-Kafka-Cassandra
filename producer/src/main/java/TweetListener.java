import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.datastax.driver.core.LocalDate;
import twitter4j.*;
import twitter4j.StatusListener;
import java.util.Date;
import java.util.Calendar;

import java.util.Properties;
import java.util.logging.Logger;

public class TweetListener implements StatusListener {
    private static final Logger logger = Logger.getLogger(TweetListener.class.getName());

    private KafkaProducer<String,Tweet> producer;
    private String topicName = "tweets-input";
    private String boostrap_server = "localhost:8080";

    public static LocalDate getDate(Date date) {
        Calendar cl = Calendar.getInstance();
        cl.setTime(date);
        LocalDate dt = LocalDate.fromYearMonthDay(cl.get(Calendar.YEAR), date.getMonth(), date.getDay());
        return dt;
    }

    public TweetListener() {
        // Criar as propriedades do produtor
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrap_server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TweetSerializer.class.getName());
        // Criar o produtor
        this.producer = new KafkaProducer<>(properties);
    }

    public void close() {
        producer.close();
    }

    @Override
    public void onStatus(Status status) {
        Tweet twtRef = new Tweet(status.getId(), getDate(status.getCreatedAt()), status.getUser().getScreenName(),
                status.getText(), status.getGeoLocation(),
                status.getSource(), status.isTruncated(), status.isFavorited());

        System.out.println("TweetProduced = " + twtRef.getId() + ", "
                + twtRef.getTweetDate() + ", "
                + twtRef.getUsername() + ", "
                + twtRef.getTweetText() + ", "
                + twtRef.getSource() + ", "
                + twtRef.isTruncated() + ", "
                + twtRef.isFavorited());
        // Enviar as mensagens
        ProducerRecord<String, Tweet> record = new ProducerRecord<>( topicName, twtRef);
        producer.send(record);
        logger.info(twtRef.getLang() + "--> " + twtRef.toString());
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int i) {
    }

    @Override
    public void onScrubGeo(long l, long l1) {

    }

    @Override
    public void onStallWarning(StallWarning stallWarning) {

    }

    @Override
    public void onException(Exception e) {

    }
}