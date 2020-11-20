import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Properties;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class CarDataProducer {
    public static KafkaProducer producer;
    public static String topic;
    public static String serverHost;
    //public static String basePath = new File("").getAbsolutePath();
    public static Path basePath = new File("").toPath();
    public static JsonObject config;
    public static int count;
    public static String path;
    public static void main(String[] args) throws Exception {

//
        topic = args[0];//"testDataTopic";//System.getenv("topic");
        serverHost = args[1];//"192.168.1.2:9092";//System.getenv("serverHost");
        count = Integer.parseInt(args[2]);//80;//Integer.parseInt(System.getenv("count"));
        path = args[3];//"C:\\Users\\Nikola\\IdeaProjects\\car_sample_generator\\testdata.txt";//args[3];
        configureProducer();
        producingAndSendingData();

    }

    public static void producingAndSendingData() throws Exception
    {
        File f = new File(path);
        FileReader fr = new FileReader(f);
        BufferedReader reader = new BufferedReader(fr);
        while (true) {
            int c=count;
            String lines="";
            String line;
            do{
                line = reader.readLine();
                lines+=line;
                lines+="\n";
                //producer.send(new ProducerRecord<>(topic,line));
            }
            while(line!=null && --c>0);
            if(line==null)
                break;

            // ProducerRecord<String, String> rec = new ProducerRecord<>(topic,  line);
            System.out.println(lines);
            // Send the record to the producer client library.
            producer.send(new ProducerRecord<>(topic,lines));
            Thread.sleep(600l);

        }
    }

    public static void configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers",serverHost);

        producer = new KafkaProducer<>(props);
    }
}
