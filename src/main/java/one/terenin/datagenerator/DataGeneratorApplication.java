package one.terenin.datagenerator;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import one.terenin.datagenerator.common.OzoneNames;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

@Slf4j
@AllArgsConstructor
@SpringBootApplication
public class DataGeneratorApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext run = SpringApplication.run(DataGeneratorApplication.class, args);
        run.addApplicationListener((ContextClosedEvent event) -> {
            OzoneClient ozoneClient = event.getApplicationContext().getBean("ozoneClient", OzoneClient.class);
            // not null in any case, class cast exception will be thrown in any other cases
            try {
                ozoneClient.close();
            } catch (IOException e) {
                // do nothing - stay it for shutdown hook
                log.info("Could not close OzoneClient from context destroy event, will be closed by shutdown hook", e);
            }
        });
        run.addApplicationListener((ContextRefreshedEvent event) -> {
            OzoneClient ozoneClient = event.getApplicationContext().getBean("ozoneClient", OzoneClient.class);
            ObjectStore objectStore = ozoneClient.getObjectStore();
            try {
                if (objectStore.getVolume(OzoneNames.ozoneVolumeName) == null) {
                    objectStore.createVolume(OzoneNames.ozoneVolumeName);
                }
                OzoneVolume standart = objectStore.getVolume(OzoneNames.ozoneVolumeName);
                if (standart.getBucket(OzoneNames.ozoneBucketName) == null) {
                    standart.createBucket(OzoneNames.ozoneBucketName);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
