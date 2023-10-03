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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.JSONObject;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;


public class Main {
    public static void main(String[] args) throws org.json.simple.parser.ParseException {
        System.out.println("Hello world!");

        userMenu();

    }
    public static void userMenu() throws org.json.simple.parser.ParseException {

        String userChoice = "";

        do {
            printMenu();

            Scanner scan = new Scanner(System.in);
            System.out.println("Gör ett följande val");
            userChoice = scan.nextLine();

            switch (userChoice){
                case "1":
                    userInputForKafka();
                    break;
                case"2":
                    getDataFromKafka("demoguides_json");
                    break;
                case"0":
                    System.out.println("Avslutar programmet");
                default:
                    break;

            }
            if(!userChoice.equals("0")){
                System.out.println("Press any key to continue");
                scan.nextLine();
            }
        } while (!userChoice.equals("0"));

    }

    private static void printMenu() {

        System.out.println("Gör ett val");
        System.out.println("1. Skriv data i Kafka Servern");
        System.out.println("2. Hämta data från Kafka Server");
        System.out.println("0. Avsluta programmet");

    }
    private static void userInputForKafka() {

        User user = new User();

        /*Logik för att låta användaren mata in data*/

        user.setMovieTitle("Insidous");
        user.setMovieGenre("Horror");
        user.setReleaseDate(2010);

        JSONObject myObj = new JSONObject();
        myObj.put("movieTitle", user.getMovieTitle());
        myObj.put("movieGenre", user.getMovieGenre());
        myObj.put("releaseDate", user.getReleaseDate());

        //URL url = new URL("http://localhost:8080/api/v1/kafka/publish");

        //Skicka Payload via WebAPI via en Request
        sendToWebApi(myObj);

    }

    public static String sendToWebApi(JSONObject myObj){
        String returnResp = "";
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
                    returnResp = responseString;
                }
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) { e.printStackTrace(); }
        return returnResp;

    }

    public static ArrayList<User> getDataFromKafka(String topicName) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fetchingGroup");

        //props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonDeserializer");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put("spring.json.trusted.packages", "*");


        Consumer<String, User> consumer = new KafkaConsumer<>(props);
        //consumer.subscribe(Collections.singletonList(topicName));
        consumer.assign(Collections.singletonList(new TopicPartition(topicName, 0)));

        //Gå till början av Topic
        consumer.seekToBeginning(consumer.assignment());

        //Skapa User lista
        ArrayList<User> users = new ArrayList<User>();


        //WhileLoop osm hämtar i JSON format
        while (true) {
            ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(100));
            if(records.isEmpty()) continue;
            for (ConsumerRecord<String, User> record : records) {
                users.add(record.value());


            }
            break;
        }
        for(User user : users){
            System.out.println(user.getMovieTitle());
            System.out.println(user.getMovieGenre());
            System.out.println(user.getReleaseDate());

        }
        return users;


    }

}