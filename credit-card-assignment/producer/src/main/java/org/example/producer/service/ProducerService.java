package org.example.producer.service;


import org.example.producer.dto.CreditCard;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ProducerService {

    @Autowired
    private KafkaTemplate<String , CreditCard> kafkaTemplate;

    public void sendCreditCardData(CreditCard card)
    {
        // topic, key, value
        kafkaTemplate.send("credit-card-data", card.getFullName(), card);

    }


}
