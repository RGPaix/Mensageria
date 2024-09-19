package com.mensageria;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Servidor {
    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) {
        // Configuração do Produtor Kafka (para enviar o status da compra)
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        // Configuração do Consumidor Kafka (para receber as mensagens de compra)
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "mensageria-servidor");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("compra"));

        // Processa as mensagens recebidas no tópico "compra"
        System.out.println("Servidor pronto para processar pedidos de compra");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            records.forEach(record -> {
                String pedido = record.value();
                System.out.println("Compra recebida: " + pedido);

                // Simular processamento da compra (por exemplo, verificar estoque)
                String statusCompra;
                if (pedido.equalsIgnoreCase("Banana") || pedido.equalsIgnoreCase("Maçã") || pedido.equalsIgnoreCase("Abacaxi") || pedido.equalsIgnoreCase("Pera" ) || pedido.equalsIgnoreCase("Uva") || pedido.equalsIgnoreCase("Morango")) {
                    statusCompra = "Compra aprovada";
                } else {
                    statusCompra = "Compra recusada: Produto não encontrado";
                }

                // Enviar o status de volta ao cliente via Kafka (tópico "status")
                ProducerRecord<String, String> statusRecord = new ProducerRecord<>("status", statusCompra);
                producer.send(statusRecord, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Erro ao enviar a mensagem: " + exception.getMessage());
                    } else {
                        System.out.println("Status da compra enviado: " + statusCompra);
                    }
                });
                producer.flush();
            });
        }
    }
}

