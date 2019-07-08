import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.datastax.driver.core.LocalDate;
import twitter4j.*;

import java.util.Properties;
import java.util.logging.Logger;

public class TweetListener implements StatusListener {
    private static final Logger logger = Logger.getLogger(TweetListener.class.getName());

    private KafkaProducer<String,Tweet> producer;
    private String topicName = "tweets-input";
    private String boostrap_server = "localhost:9092";

    public static LocalDate getDate(String date) {
        java.time.LocalDate jtld = java.time.LocalDate.parse(date);
        return LocalDate.fromYearMonthDay(jtld.getYear(), jtld.getMonthValue(), jtld.getDayOfMonth());
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
        Tweet t = new Tweet(status.getId(), getDate(status.getCreatedAt().toString()), status.getUser().getScreenName(),
                status.getText(), status.getGeoLocation().getLatitude(), status.getGeoLocation().getLongitude(),
                status.getSource(), status.isTruncated(), status.isFavorited());
        // Enviar as mensagens
        ProducerRecord<String, Tweet> record = new ProducerRecord<>( topicName, t);
        producer.send(record);
        //logger.info(t.getLanguage() + "--> " + t.toString());
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