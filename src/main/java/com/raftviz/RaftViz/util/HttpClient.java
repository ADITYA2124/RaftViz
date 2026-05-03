package com.raftviz.RaftViz.util;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

public class HttpClient {
    private final RestTemplate rt;

    public HttpClient() {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(300));
        requestFactory.setReadTimeout(Duration.ofMillis(700));
        this.rt = new RestTemplate(requestFactory);
    }


    public <T> T post(String url, Object body, Class<T> clazz) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<T> resp = rt.postForEntity(url, new HttpEntity<>(body, h), clazz);
        return resp.getBody();
    }


    public <T> T get(String url, Class<T> clazz) {
        return rt.getForObject(url, clazz);
    }
}
