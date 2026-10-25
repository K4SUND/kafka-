package org.example.userproducer.controller;


import org.example.userproducer.dto.User;
import org.example.userproducer.service.UserProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/user-api")
public class UserController {

    @Autowired
    private UserProducerService userProducerService;

    /*
    @PostMapping("/publishUser/{name}/{age}")
    public void sendUserData(@PathVariable("name") String name, @PathVariable("age") int age)
    {
        userProducerService.sendUserData(name,age);
    }

     */

    @PostMapping("/publishUser")
    public void sendUserData(@RequestBody  User user)
    {
        userProducerService.sendUserData(user);
    }

}
