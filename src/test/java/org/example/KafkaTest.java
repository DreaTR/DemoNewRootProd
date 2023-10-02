package org.example;

import com.andrea.demo.payload.User;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.*;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaTest {

    private static User user;

    private static JSONObject myObj;

    @BeforeAll
    static void beforeAll() {
        user = new User();
        user.setMovieTitle("Terminator");
        user.setMovieGenre("Science Fiction");
        user.setReleaseDate(1984);

        myObj = new JSONObject();

        myObj.put("movieTitle", user.getMovieTitle());
        myObj.put("movieGenre", user.getMovieGenre());
        myObj.put("releaseDate", user.getReleaseDate());

    }
    @Test
    @Order(1)
    public void sendToWebAPITest() {
        //Anropa metod för att skicka den
        String resp = Main.sendToWebApi(myObj);

        //Jämföra response-värden
        assertEquals(resp, "Json Message sent to Kafka Topic");
    }
    @Test
    @Order(2)
    public void getDataFromKafkaTest() {
        //Anropa metod för att hämta Users
        ArrayList<User> users = Main.getDataFromKafka("demoguides_json");
        User testUser = users.get(users.size() - 1);

        assertEquals( testUser.getMovieTitle() , user.getMovieTitle());
        assertEquals( testUser.getMovieGenre() , user.getMovieGenre());
        assertEquals( testUser.getReleaseDate() , user.getReleaseDate());

    }

}