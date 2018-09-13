package com.jwszol;

import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SqlSparkTest {

    private transient SparkSession ss;

    @Before
    public void setUp() {
        ss = SparkSession
                .builder()
                .appName("Spark SQL Session")
                .config("spark.master", "local")
                .getOrCreate();
    }

    @After
    public void close(){
        ss.stop();
    }

    @Test
    public void testSQLJob() {
        SqlSparkJob sj = new SqlSparkJob(ss);
        sj.getSchema();
        sj.getEmail();
        sj.selectUsersGt2(sj.addValueToId());
        sj.printTransSchema();
        sj.joinData();
        sj.productsBought();
        sj.productsBoughtLocation();
        sj.bestProduct();
    }

}
