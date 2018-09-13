package com.jwszol;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

public class JoinJobTest { ;
    private transient JavaSparkContext sc;

    @Before
    public void setUp() {
        sc = new JavaSparkContext("local", "SparkJoinsTest");
    }

    @After
    public void tearDown() {
        if (sc != null){
            sc.stop();
        }
    }

    @Test
    public void testExampleJob() {
        TransactionAnalyseJob job = new TransactionAnalyseJob(sc);
        JavaPairRDD<Integer, Tuple2<Integer, String>>results = job.countAmountByLocation("./src/main/resources/transactions.txt", "./src/main/resources/users.txt");

        System.out.println(results.collect().get(0)._1 + " " + results.collect().get(0)._2 );
        System.out.println(results.collect().get(1)._1 + " " + results.collect().get(1)._2 );
        System.out.println(results.collect().get(2)._1 + " " + results.collect().get(2)._2 );

    }
}
