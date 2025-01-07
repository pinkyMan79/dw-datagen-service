package one.terenin.datagenerator.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import one.terenin.datagenerator.generator.parent.DataGenerator;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@ConditionalOnProperty("datagen.ozone")
public class LoaderService {

    private final List<DataGenerator> generators;

    @Value("${data.gen.count}")
    private Integer countToGen;

    private final OzoneClient client;

    public LoaderService(List<DataGenerator> generators, OzoneClient client) {
        this.generators = generators;
        this.client = client;
    }

    public void load() {
        log.info("Generate new data and load it to Apache Ozone, current data.gen.count: {}", countToGen);
        generators.forEach(it -> {
            try {
                it.generate(countToGen, client);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    //FIXME: add shutdown
    public void loadWithFixedDelay() {
        new ScheduledThreadPoolExecutor(1).scheduleWithFixedDelay(this::load, 1, 10, TimeUnit.MINUTES);
    }

}
