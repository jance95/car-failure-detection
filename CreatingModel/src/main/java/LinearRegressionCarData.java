import java.io.IOException;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.nio.file.Path;
import java.io.*;

import org.apache.spark.ml.PipelineModel;


public class LinearRegressionCarData {
public static KafkaConsumer consumer;
    public static Path basePath = new File("").toPath();
    public static void main(String[] args) throws IOException {
        String path = args[1];//"C:\\Users\\Nikola\\IdeaProjects\\CreatingModel\\src\\data\\testdata.txt";
        String modelPath = args[2];//"hdfs://localhost:9000/user/Nikola/jana/car-model"
        //String path = /*basePath.resolve("testdata.txt").toString()*/"C:\\Users\\Nikola\\IdeaProjects\\CreatingModel\\src\\data\\testdata.txt";

        SparkConf conf = new SparkConf().setAppName("appName").setMaster(args[0]);
        conf.set("spark.driver.memory","524288000");
        conf.set("spark.executor.memory","524288000");
        conf.set("spark.testing.memory","524288000");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset data= spark.read().json(path);
        Dataset<Row> dataset = data.cache();
        List<String> columns = new ArrayList<String>(Arrays.asList(dataset.columns()));
        String first = columns.remove(columns.indexOf("failure_occurred"));
        StringIndexer indexer = new StringIndexer().setInputCol("failure_occurred").setOutputCol("label");
        VectorAssembler assembler =  new VectorAssembler().setInputCols(columns.subList(0,columns.size()).stream().toArray(String[]::new)).setOutputCol("features");
        LogisticRegression regression = new LogisticRegression().setMaxIter(100).setRegParam(0.02).setElasticNetParam(0.8);
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{assembler,indexer,regression});
        Dataset<Row>[] res = dataset.randomSplit(new double[] {0.6,0.4});
        PipelineModel model = pipeline.fit(res[0]);
        Dataset<Row> result = model.transform(res[1]);
        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setMetricName("areaUnderROC");
        System.out.println(evaluator.evaluate(result));
        model.write().overwrite().save(modelPath);


        //String first = columns.remove(columns.indexOf("failure_occurred"));
//        StringIndexer indexer = new StringIndexer().setInputCol("failure_occurred").setOutputCol("label");
//        //StringIndexerModel ml = indexer.fit(dataset);
//        VectorAssembler assembler =  new VectorAssembler().setInputCols(columns.subList(0,columns.size()).stream().toArray(String[]::new)).setOutputCol("features");
//        LogisticRegression regression = new LogisticRegression().setMaxIter(100).setRegParam(0.02).setElasticNetParam(0.8);
//        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{assembler,indexer,regression});
//        Dataset<Row>[] res = dataset.randomSplit(new double[] {0.6,0.4});
//        PipelineModel model = pipeline.fit(res[0]);
//        Dataset<Row> result = model.transform(res[1]);
//        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setMetricName("areaUnderROC");
//        System.out.println(evaluator.evaluate(result));
        //model.write().overwrite().save("hdfs://localhost:9000/user/Nikola/jana/car-model");

    }
}
