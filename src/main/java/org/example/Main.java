package org.example;


import com.andrea.demo.payload.User;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws org.json.simple.parser.ParseException {
        System.out.println("Hello world!");

        User user = new User();

        /*Logik flr att låta användaren mata in data*/
        user.setId(6);
        user.setFirstName("Kevin");
        user.setLastName("Hart");

        JSONObject myObj = new JSONObject();
        myObj.put("id", user.getId());
        myObj.put("firstName", user.getFirstName());
        myObj.put("lastName", user.getLastName());


        //URL url = new URL("http://localhost:8080/api/v1/kafka/publish");

        //Skicka Payload via WebAPI via en Request
        sendToWebApi(myObj);

        //Hämta data från topic
        getDataFromKafka("demoguides_json");

        System.out.println("Project done");
    }
    public static void sendToWebApi(JSONObject myObj){

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost("http://localhost:8080/api/v1/kafka/publish");

            // Skapa en JSON-förfrågningskropp
            String jsonPayload = myObj.toJSONString();
            StringEntity entity = new StringEntity(jsonPayload, ContentType.APPLICATION_JSON);
            httpPost.setEntity(entity);

            // Skicka förfrågan och hantera svaret
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                HttpEntity responseEntity = response.getEntity();
                if (responseEntity != null) {
                    String responseString = EntityUtils.toString(responseEntity);
                    System.out.println("Svar från server: " + responseString);
                }
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) { e.printStackTrace(); }

    }

    public static void getDataFromKafka(String topicName) throws org.json.simple.parser.ParseException {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fetchingGroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonDeserializer");
        props.put("spring.json.trusted.packages", "*");


        Consumer<String, User> consumer = new KafkaConsumer<>(props);
        //consumer.subscribe(Collections.singletonList(topicName));
        consumer.assign(Collections.singletonList(new TopicPartition(topicName, 0)));

        consumer.seekToBeginning(consumer.assignment());

        //WhileLoop osm hämtar i JSON format
        while (true) {
            ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, User> record : records) {
                System.out.println(record);


            }
        }


    }
}