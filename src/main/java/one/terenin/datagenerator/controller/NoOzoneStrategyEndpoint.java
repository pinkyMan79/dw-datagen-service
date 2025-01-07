package one.terenin.datagenerator.controller;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import one.terenin.datagenerator.generator.JSONDataGenerator;
import one.terenin.datagenerator.generator.ParquetDataGenerator;
import one.terenin.datagenerator.generator.parent.DataGenerator;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/datagen")
@RequiredArgsConstructor
public class NoOzoneStrategyEndpoint {

    private final List<DataGenerator<?>> generators;

    @SneakyThrows
    @GetMapping("/json/{count}")
    public ResponseEntity<String> generateJson(@PathVariable(name = "count") int count) {
        JSONDataGenerator jsonGen = (JSONDataGenerator) generators.stream()
                .filter(it -> it instanceof JSONDataGenerator)
                .findAny().orElseThrow(() -> new RuntimeException("No data generator found"));
        return ResponseEntity.ok(jsonGen.generateWithResponse(count));
    }

    @SneakyThrows
    @GetMapping(value = "/parquet/{count}", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<byte[]> generateParquet(@PathVariable(name = "count") int count) {
        ParquetDataGenerator parquetDataGenerator = (ParquetDataGenerator) generators.stream()
                .filter(it -> it instanceof ParquetDataGenerator)
                .findAny().orElseThrow(() -> new RuntimeException("No data generator found"));
        return ResponseEntity.ok(parquetDataGenerator.generateWithResponse(count));
    }
}
