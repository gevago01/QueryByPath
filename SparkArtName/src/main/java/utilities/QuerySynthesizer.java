package utilities;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.List;

/**
 * Created by giannis on 10/04/19.
 */
public class QuerySynthesizer implements Serializable {


    public static void synthesize(JavaPairRDD<Integer, Iterable<CSVRecord>> recordsCached) {
        JavaPairRDD<Integer, Iterable<CSVRecord>> recordSample = recordsCached.sample(false, 0.03);
        List<Tuple2<Integer, Iterable<CSVRecord>>> allRecords = recordSample.collect();

        System.out.println("recordSample.count():" + recordSample.groupByKey().keys().count());
        try {
            BufferedWriter bf = new BufferedWriter(new FileWriter(new File("roadSegmentSkewedQueries")));
//            BufferedWriter bf = new BufferedWriter(new FileWriter(new File("timeSkewedQueries")));

            for (Tuple2<Integer, Iterable<CSVRecord>> tuple : allRecords) {


                Iterable<CSVRecord> trajectory = tuple._2();


                for (CSVRecord csvRec : trajectory) {

                    bf.write(csvRec.getTrajID() + ", " + csvRec.getTimestamp() + ", " + csvRec.getRoadSegment() + "\n");

                }
            }

            bf.close();
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }


    }
}
