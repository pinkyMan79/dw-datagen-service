package one.terenin.datagenerator.configuration;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import one.terenin.datagenerator.configuration.property_holder.OzoneConfigurationPropertySource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;

import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.DEFAULT_SHUTDOWN_HOOK_PRIORITY;

@Slf4j
@Configuration
@AllArgsConstructor
public class GeneratorConfiguration {

    private final OzoneConfigurationPropertySource source;

    @SneakyThrows
    @Bean
    public OzoneClient ozoneClient() {
        OzoneConfiguration configuration = getOzoneConfiguration();
        OzoneClient client = OzoneClientFactory.getRpcClient(configuration);
        // may be ShutdownHookManager instead of application event ?
        ShutdownHookManager.get().addShutdownHook(() -> {
            try {
                client.close();
            } catch (Exception e) {
                log.error("Error during revoke connection to ozone cluster", e);
            }
        }, DEFAULT_SHUTDOWN_HOOK_PRIORITY);
        return client;
    }

    @SneakyThrows
    @Bean
    @ConditionalOnProperty("spring.datagen.krb.enable")
    public UserGroupInformation localUgi() {
        // I don't really want to use kerberos in that project, that's take some time to add and configure krb5(+kdc) server and clients
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        log.info("current user is: \n {} \n from keytab? {}", currentUser, currentUser.isFromKeytab());
        return currentUser;
    }

    private @NotNull OzoneConfiguration getOzoneConfiguration() {
        OzoneConfiguration configuration = new OzoneConfiguration();
        if (source.isDefaultConfigLoadingEnable()) {
            configuration.setBoolean("ozone.security.enabled", source.isOzoneSecurityEnable());
            configuration.set("ozone.om.address", source.getOmAddress());
        } else {
            // resources configuration loading
            if (new File("/etc/hadoop/conf/ozone-site.xml").exists()) {
                configuration.addResource("/etc/hadoop/conf/ozone-site.xml");
            }
            if (new File("/etc/hadoop/conf/core-site.xml").exists()) {
                configuration.addResource("/etc/hadoop/conf/core-site.xml");
            }
        }
        return configuration;
    }

}
