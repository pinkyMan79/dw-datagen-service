package one.terenin.datagenerator;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import one.terenin.datagenerator.service.LoaderService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@Slf4j
@AllArgsConstructor
@SpringBootApplication
public class DataGeneratorApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext run = SpringApplication.run(DataGeneratorApplication.class, args);
        //run.getBean("loaderService", LoaderService.class).load();
    }

}
