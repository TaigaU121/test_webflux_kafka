// public static FlinkKafkaConsumer011<String> createStringConsumerForTopic(
//   String topic, String kafkaAddress, String kafkaGroup ) {
 
//     Properties props = new Properties();
//     props.setProperty("bootstrap.servers", kafkaAddress);
//     props.setProperty("group.id",kafkaGroup);
//     FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(
//       topic, new SimpleStringSchema(), props);

//     return consumer;
// }