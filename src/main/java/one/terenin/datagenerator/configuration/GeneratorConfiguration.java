package one.terenin.datagenerator.configuration;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import one.terenin.datagenerator.configuration.property.OzoneConfigurationPropertySource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@Slf4j
@Configuration
@AllArgsConstructor
public class GeneratorConfiguration {

    private final OzoneConfigurationPropertySource source;

    @SneakyThrows
    @Bean
    public OzoneClient ozoneClient() {
        OzoneConfiguration configuration = new OzoneConfiguration();
        if (source.isDefaultConfigLoadingEnable()) {
            //... any ozone opts here
        } else {
            // resources configuration loading
            if (new File("/etc/hadoop/conf/ozone-site.xml").exists()) {
                configuration.addResource("/etc/hadoop/conf/ozone-site.xml");
            }
            if (new File("/etc/hadoop/conf/core-site.xml").exists()) {
                configuration.addResource("/etc/hadoop/conf/core-site.xml");
            }
        }
        OzoneClient client = OzoneClientFactory.getRpcClient(configuration);
        // may be ShutdownHookManager instead of application event ?
        return client;
    }

    @SneakyThrows
    @Bean
    @ConditionalOnProperty("spring.datagen.krb.enable")
    public UserGroupInformation localUgi() {
        // i don't really want to use kerberos in that project, that's take some time to add and configure krb5(+kdc) server and clients
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        log.info("current user is: \n {} \n from keytab? {}", currentUser, currentUser.isFromKeytab());
        return currentUser;
    }

}
