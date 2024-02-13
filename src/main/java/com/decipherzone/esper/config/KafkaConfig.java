package com.decipherzone.esper.config;

public class KafkaConfig {
    public final static String[] INPUT_TOPICS = {"A","B","C","input"};
    public final static String[] OUTPUT_TOPICS = {"A_out","B_out","C_out","wordLength"};
    public final static String BOOTSTRAP_ADDRESS = "localhost:9092";
}
