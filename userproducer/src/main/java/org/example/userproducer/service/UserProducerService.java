package org.example.userproducer.service;


import org.example.userproducer.dto.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class UserProducerService {

    //inject kafka template
    // key - String
    //value - Integer
//    @Autowired
//    private KafkaTemplate<String ,Integer> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String ,User> kafkaTemplate;


    /*
    public void sendUserData(String name,int age)
    {
        // topic, key, value
        kafkaTemplate.send("user-data",name,age);

    }

     */


    public void sendUserData(User user)
    {
        // topic, key, value
        kafkaTemplate.send("user-data", user.getName(), user);

    }


}
