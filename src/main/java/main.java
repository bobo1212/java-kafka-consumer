import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class main {
    public static void main(String[] args) throws Exception {
        if(args.length < 2){
            System.out.println("Enter topic name and servers adres");
            return;
        }
        //Kafka consumer configuration settings
        String topicName = args[0].toString();
        String server = args[1].toString();
        Properties props = new Properties();

        props.put("bootstrap.servers", server);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer
                <String, String>(props);

        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicName));

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);
        int i = 0;
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                Date date = new Date();
                // print the offset,key and value for the consumer records.
                System.out.printf("[%s] offset = %d, key = %s, value = %s\n", dateFormat.format(date), record.offset(), record.key(), record.value());
            }
        }
    }
}
