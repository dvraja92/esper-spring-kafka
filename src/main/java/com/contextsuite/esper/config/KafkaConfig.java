package com.contextsuite.esper.config;

public class KafkaConfig {
    public final static String[] INPUT_TOPICS = {"A","B"};
    public final static String[] OUTPUT_TOPICS = {"A_out","B_out"};
    public final static String BOOTSTRAP_ADDRESS = "localhost:9092";
}
