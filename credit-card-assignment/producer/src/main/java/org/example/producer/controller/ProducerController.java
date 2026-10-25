package org.example.producer.controller;


import org.example.producer.dto.CreditCard;
import org.example.producer.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/credit-card-api")
public class ProducerController {

    @Autowired
    private ProducerService producerService;

    @PostMapping("/publish")
    public void sendUserData(@RequestBody CreditCard card)
    {
        producerService.sendCreditCardData(card);
    }

}
