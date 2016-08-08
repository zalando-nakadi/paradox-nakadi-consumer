package de.zalando.paradox.nakadi.consumer.example.boot;

import org.junit.Test;

import org.junit.runner.RunWith;

import org.springframework.boot.test.SpringApplicationConfiguration;

import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@ActiveProfiles("junit")
public class ApplicationTests {

    @Test
    public void contextLoads() { }

}
