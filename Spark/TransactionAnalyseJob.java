package com.jwszol;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class TransactionAnalyseJob {
    private static class Transaction{
        private int transactionID;
        private int productID;
        private int userID;
        private int amount;
        private String description;
        Transaction(String rawTransactionRow) {
            String[] values = rawTransactionRow.split("\t");
            this.transactionID = Integer.valueOf(values[0]);
            this.productID = Integer.valueOf(values[1]);
            this.userID = Integer.valueOf(values[2]);
            this.amount = Integer.valueOf(values[3]);
            this.description = values[4];
        }

        public String getDescription() {
            return description;
        }

        public int getUserID() {
            return userID;
        }

        public int getAmount() {
            return amount;
        }

        public int getTransactionID() {
            return transactionID;
        }

        public int getProductID() {
            return productID;
        }
    }
    private static class User{
        private int userID;
        private String email;
        private String language;
        private String location;

        User(String rawUserRow){
            String[] values = rawUserRow.split("\t");
            this.userID = Integer.valueOf(values[0]);
            this.email = values[1];
            this.language = values[2];
            this.location = values[3];
        }

        public int getUserID() {
            return userID;
        }

        public String getEmail() {
            return email;
        }

        public String getLanguage() {
            return language;
        }

        public String getLocation() {
            return location;
        }
    }

    private static JavaSparkContext sc;

    public TransactionAnalyseJob(JavaSparkContext sc){
        this.sc = sc;
    }

    public static JavaPairRDD<Integer, Tuple2<Integer, String>> countAmountByLocation(String transactionsFile, String usersFile){
        JavaRDD<String> rawTransactions = sc.textFile(transactionsFile);
        JavaRDD<String> rawUsers = sc.textFile(usersFile);

        JavaPairRDD<Integer, Integer> transactionWithUserPair =
                rawTransactions.mapToPair(
                        new PairFunction<String, Integer, Integer>() {
                            @Override
                            public Tuple2<Integer, Integer> call(String s) {
                                Transaction transaction = new Transaction(s);
                                return new Tuple2<>(transaction.getTransactionID(), transaction.getUserID());
                            }
                        });

        JavaPairRDD<Integer, Integer> transactionWithAmountPair =
                rawTransactions.mapToPair(
                        new PairFunction<String, Integer, Integer>() {
                            @Override
                            public Tuple2<Integer, Integer> call(String s) {
                                Transaction transaction = new Transaction(s);
                                return new Tuple2<>(transaction.getTransactionID(), transaction.getAmount());
                            }
                        });

        JavaPairRDD<Integer, String> userWithLocationPair =
                rawUsers.mapToPair(
                        new PairFunction<String, Integer, String>() {
                            @Override
                            public Tuple2<Integer, String> call(String s) {
                                User user = new User(s);
                                return new Tuple2<>(user.getUserID(), user.getLocation());
                            }
                        });
        JavaPairRDD<Integer, Integer> userWithAmountPair =
                transactionWithUserPair.join(transactionWithAmountPair)
                .values()
                .mapToPair(
                        new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
                            @Override
                            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                                return integerIntegerTuple2;
                            }
                        }
                )
                .reduceByKey(
                        new Function2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer value1, Integer value2) throws Exception {
                                return value1 + value2;
                            }
                        }
                );
        return userWithAmountPair.join(userWithLocationPair);
    }
}
