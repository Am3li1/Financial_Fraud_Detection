// src/main/java/com/yourcompany/TransactionMonitoringJob.java
package com.yourcompany;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


public class TransactionMonitoringJob {
    
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.enableCheckpointing(5000);
        
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("transactions_institution1", "transactions_institution2", "transactions_institution3")
            .setGroupId("flink-transaction-monitoring")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        DataStream<String> kafkaTransactions = env.fromSource(
            source,
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
            "Kafka Source");
        
        DataStream<Transaction> transactions = kafkaTransactions
            .map(new ParseJSON());
            
        DataStream<Transaction> timestampedTransactions = transactions
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner(new TimestampAssignerSupplier<Transaction>() {
                @Override
                public TimestampAssigner<Transaction> createTimestampAssigner(Context context) {
                    return (event, timestamp) -> event.getTimestamp();
                }
            })
    );
        
        // <<< FIX: REPLACED LAMBDAS WITH EXPLICIT FUNCTIONS
        timestampedTransactions
            .filter(new FilterFunction<Transaction>() {
                @Override
                public boolean filter(Transaction t) throws Exception {
                    return t.getAmount() > 5000;
                }
            })
            .map(new MapFunction<Transaction, String>() {
                @Override
                public String map(Transaction t) throws Exception {
                    return String.format("🚨 Large transaction alert: $%,.2f from %s", 
                            t.getAmount(), t.getInstitutionId());
                }
            })
            .print();
        
        // <<< FIX: REPLACED LAMBDAS WITH EXPLICIT FUNCTIONS
        timestampedTransactions
            .keyBy(new KeySelector<Transaction, String>() {
                @Override
                public String getKey(Transaction transaction) throws Exception {
                    return transaction.getInstitutionId();
                }
            })
            .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
            .apply(new WindowFunction<Transaction, String, String, TimeWindow>() {
                @Override
                public void apply(String institutionId, TimeWindow window, Iterable<Transaction> txs, Collector<String> out) throws Exception {
                    double sum = 0;
                    int count = 0;
                    for (Transaction t : txs) {
                        sum += t.getAmount();
                        count++;
                    }
                    if (sum > 10000) {
                        out.collect(String.format(
                            "⚠️ Transaction spike detected for %s: $%,.2f in last 5min (%d transactions)",
                            institutionId, sum, count));
                    }
                }
            })
            .print();
        
        env.execute("Transaction Monitoring");
    }
    
    public static class ParseJSON implements MapFunction<String, Transaction> {
        @Override
        public Transaction map(String value) throws Exception {
            JSONObject json = new JSONObject(value);
            
            String timestampStr = json.getString("timestamp");
            long timestampMillis = LocalDateTime.parse(timestampStr)
                                                .toInstant(ZoneOffset.UTC)
                                                .toEpochMilli();

            return new Transaction(
                json.getString("transaction_id"),
                json.getString("institution_id"),
                json.getDouble("amount"),
                timestampMillis
            );
        }
    }
    
    public static class Transaction {
        private String transactionId;
        private String institutionId;
        private double amount;
        private long timestamp;
        
        public Transaction(String transactionId, String institutionId, double amount, long timestamp) {
            this.transactionId = transactionId;
            this.institutionId = institutionId;
            this.amount = amount;
            this.timestamp = timestamp;
        }
        
        public String getTransactionId() { return transactionId; }
        public String getInstitutionId() { return institutionId; }
        public double getAmount() { return amount; }
        public long getTimestamp() { return timestamp; }
    }
}