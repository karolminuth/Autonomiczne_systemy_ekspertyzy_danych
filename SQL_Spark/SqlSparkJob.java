package com.jwszol;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import scala.Int;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;


public class SqlSparkJob {

    private Dataset<Row> dfUsers = null;
    private Dataset<Row> dfTrans = null;
    private Dataset<Row> dfMeta = null;
    
    private SparkSession spark = null;

    public SqlSparkJob(SparkSession spark){
        this.spark = spark;
        this.dfUsers = this.spark.read().option("delimiter","|").option("header","true").csv("./src/main/resources/users.txt");
        this.dfTrans = this.spark.read().option("delimiter","|").option("header","true").csv("./src/main/resources/transactions.txt");
        this.dfMeta = this.spark.read().option("delimiter","|").option("header","true").csv("./src/main/resources/prod-meta.txt");
    }

    public void getSchema(){
        dfUsers.printSchema();
    }

    public void getEmail(){
        dfUsers.select(col("email")).show();
    }

    public Dataset<Row> addValueToId(){
        // Register the DataFrame as a SQL temporary view
        Dataset<Row> tempdfUsers =  dfUsers.select(col("email"), col("id").plus(1).name("new_id"));
        tempdfUsers.registerTempTable("users2");

        Dataset<Row> sqldfUsers = this.spark.sql("SELECT * FROM users2");
        sqldfUsers.show();
        return sqldfUsers;
    }

    public void selectUsersGt2(Dataset<Row> users){
        users.createOrReplaceTempView("users3");

        Dataset<Row> limitedUsersList = this.spark.sql("SELECT * from users3 WHERE new_id > 2");
        limitedUsersList.show();
    }

    public void printTransSchema(){
        this.dfTrans.show();
    }

    public void joinData(){
        Dataset<Row> joinDs = this.dfTrans.join(dfUsers, this.dfTrans.col("user_id").equalTo(this.dfUsers.col("id")),"leftouter");
        joinDs.show();
    }

    public void productsBought() {
        List<Row> indexes_of_users = dfTrans.select(col("user_id")).distinct().collectAsList();
        List<Long> how_many_times_bought = new ArrayList<Long>();
        for(Row id: indexes_of_users)
        {
            String condition = "user_id == " + id.get(0).toString();
            long counter = dfTrans.select(col("prod_id")).where(condition).count();
            how_many_times_bought.add(counter);
        }

        System.out.print("User_id\t\t");
        System.out.println("Bought");

        for(int i = 0; i < indexes_of_users.size(); i++)
        {
            System.out.print(indexes_of_users.get(i).get(0));
            System.out.print("\t\t\t");
            System.out.println(how_many_times_bought.get(i));
        }

    }

    public void productsBoughtLocation() {
        Dataset<Row> joinDf = this.dfTrans.join(dfUsers, this.dfTrans.col("user_id").equalTo(this.dfUsers.col("id")),"leftouter");
        List<Row> locations = dfUsers.select(col("location")).distinct().collectAsList();

        List<Long> how_many_times_bought_in_locations = new ArrayList<Long>();

        for(Row location: locations)
        {
            String condition = "location == \"" + location.get(0).toString() +"\"";
            long counter = joinDf.select(col("trans_id")).where(condition).count();
            how_many_times_bought_in_locations.add(counter);
        }

        System.out.print("Location\t");
        System.out.println("Bought");

        for(int i = 0; i < locations.size(); i++)
        {
            System.out.print(locations.get(i).get(0));
            System.out.print("\t\t\t");
            System.out.println(how_many_times_bought_in_locations.get(i));
        }

    }
    
    public void bestProduct(){
        
        Dataset<Row> joinDf = this.dfTrans.join(dfUsers, this.dfTrans.col("user_id").equalTo(this.dfUsers.col("id")),"leftouter");
        joinDf = joinDf.join(dfMeta, joinDf.col("prod_id").equalTo(this.dfMeta.col("product-id")),"leftouter");
        List<Row> locations = dfUsers.select(col("location")).distinct().collectAsList();
        List<Row> products = joinDf.select(col("prod_id")).distinct().collectAsList();
        
        List<Integer> result = new ArrayList<>();
        int index_of_product = 0;
        long max_value_of_appearances = -1;
        
        for(Row location: locations)
        {
            for(Row product: products)
            {
                String condition = "location == \"" + location.get(0).toString() +
                        "\" AND prod_id == \"" + product.get(0).toString() + "\"";

                String value = product.get(0).toString();
                int new_value = Integer.parseInt(value);

                long how_many_times_this_product = joinDf.select(col("prod_id")).where(condition).count();

                if(how_many_times_this_product > max_value_of_appearances)
                {
                    max_value_of_appearances = how_many_times_this_product;
                    index_of_product = new_value;
                }
            }
            
            result.add(index_of_product);
            index_of_product = 0;
            max_value_of_appearances = -1;
        }
        
        List<String> names_of_product = new ArrayList<>();
        
        for(Integer product: result)
        {
            String condition = "prod_id == \"" + product.toString() + "\"";
            List<Row> name = joinDf.select(col("name")).where(condition).collectAsList();
            names_of_product.add(name.get(0).toString());
        }
        
        System.out.print("Location\t");
        System.out.println("Item");
        
        for(int i = 0; i < names_of_product.size(); i++)
        {
            System.out.print(locations.get(i).get(0));
            System.out.print("\t\t\t");
            System.out.println(names_of_product.get(i));
        }
    }
}
