package com.raftviz.RaftViz.rpc;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class HttpClient {
    private final RestTemplate rt = new RestTemplate();
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