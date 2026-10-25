package org.example.userconsumer.service;


import org.example.userconsumer.dto.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class UserConsumerService {

    // kafka listener annotation
    @KafkaListener(topics = {"user-data"})
    public void consumeUserData(User user)
    {
        System.out.println("Users Age is "+user.getAge()+" and Town is "+user.getTown());

    }
}
