package com.mensageria;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Cliente {
    private static KafkaProducer<String, String> producer;
    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        // Configuração do Produtor Kafka
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        // Configuração do Consumidor Kafka
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "mensageria-cliente");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("status"));

        // Criar a interface gráfica
        JFrame frame = new JFrame("Sistema de Compras");
        frame.setSize(400, 400);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        JLabel label = new JLabel("Digite o nome do produto:");
        JTextField textField = new JTextField(20);
        JButton comprarButton = new JButton("Comprar");
        JTextArea statusArea = new JTextArea(5, 20);

        JPanel panel = new JPanel();
        panel.add(label);
        panel.add(textField);
        panel.add(comprarButton);
        panel.add(statusArea);

        frame.add(panel);
        frame.setVisible(true);

        // Ação do botão Comprar
        comprarButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String pedidoCompra = textField.getText();
                if (!pedidoCompra.isEmpty()) {
                    // Enviar pedido de compra para o Kafka (tópico "compra")
                    ProducerRecord<String, String> record = new ProducerRecord<>("compra", pedidoCompra);
                    producer.send(record);
                    producer.flush();
                    statusArea.append("Pedido de compra enviado: " + pedidoCompra + "\n");

                    // Limpar o campo de texto
                    textField.setText("");
                }
            }
        });

        // Thread para receber as mensagens do servidor
        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(record -> {
                    // Exibir o status da compra na área de texto
                    statusArea.append("Status da compra recebido: " + record.value() + "\n");
                });
            }
        }).start();
    }
}
