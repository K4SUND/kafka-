package org.example.consumer.service;


import org.example.consumer.dto.CreditCard;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    // kafka listener annotation
    @KafkaListener(topics = {"credit-card-data"})
    public void consumeUserData(CreditCard creditCard)
    {
        System.out.println("Full name:"+ creditCard.getFullName());
        System.out.println("Card number:"+ creditCard.getCardNumber());
        System.out.println("Exp date:"+ creditCard.getExpDate());
        System.out.println("Sec code:"+ creditCard.getSecCode());

    }
}
