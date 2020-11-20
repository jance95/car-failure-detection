import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.from_json;

public class CarFailurePrediction {
    public static void main(String[] args) throws IOException, StreamingQueryException {
        String path = args[1];//"C:\\Users\\Nikola\\IdeaProjects\\CreatingModel\\src\\data\\testdata.txt";
        String modelPath = args[2];//"hdfs://localhost:9000/user/Nikola/jana/car-model"
        String server =args[3];//"192.168.1.2:9092"
        String topic = args[4];//"testDataTopic"
        SparkConf conf = new SparkConf().setAppName("CarFailurePrediction").setMaster(args[0]);
        conf.set("spark.driver.memory","524288000");
        conf.set("spark.executor.memory","524288000");
        conf.set("spark.testing.memory","524288000");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset data= spark.read().json(path);
        Dataset<Row> dataset = data.cache();
        List<String> columns = new ArrayList<String>(Arrays.asList(dataset.columns()));

        PipelineModel model = PipelineModel.load(modelPath);

        StructField[] fields = new StructField[columns.size()];
        Integer i=0;
        for(String s:columns) {
            fields[i++]= new StructField(s, DataTypes.DoubleType,true, Metadata.empty());
        }
        StructType schema =new StructType(fields);
        Dataset<Row> datasetTest = spark.readStream().format("kafka").option("kafka.bootstrap.servers", server).option("subscribe", topic).load();
        Dataset<String> rr = datasetTest.selectExpr("CAST(value AS STRING)").as(Encoders.STRING()).flatMap((FlatMapFunction<String, String>) x-> Arrays.asList(x.split("\n")).iterator(),Encoders.STRING());
        Dataset<Row> novi=rr.select(from_json(new Column("value"), schema).as("data")).select("data.*");
        Dataset<Row> result = model.transform(novi);
        StreamingQuery query = result.writeStream().outputMode("append").format("console").start();
        query.awaitTermination();
    }
}
