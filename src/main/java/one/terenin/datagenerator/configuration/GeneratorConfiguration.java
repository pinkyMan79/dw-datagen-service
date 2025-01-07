package one.terenin.datagenerator.configuration;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import one.terenin.datagenerator.common.OzoneNames;
import one.terenin.datagenerator.configuration.property_holder.OzoneConfigurationPropertySource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.util.ShutdownHookManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.ozone.conf.OzoneServiceConfig.DEFAULT_SHUTDOWN_HOOK_PRIORITY;

@Slf4j
@Configuration
@AllArgsConstructor
@ConditionalOnProperty("datagen.ozone")
public class GeneratorConfiguration {

    private final OzoneConfigurationPropertySource source;

    @SneakyThrows
    @Bean
    public OzoneClient ozoneClient() {
        OzoneConfiguration configuration = getOzoneConfiguration();
        OzoneClient client = OzoneClientFactory.getRpcClient(configuration);
        ShutdownHookManager.get().addShutdownHook(() -> {
            try {
                client.close();
            } catch (Exception e) {
                log.error("Error during revoke connection to ozone cluster", e);
            }
        }, DEFAULT_SHUTDOWN_HOOK_PRIORITY);
        return client;
    }

    @Bean
    public OzoneVolume ozoneVolume(OzoneClient client) {
        ObjectStore objectStore = client.getObjectStore();
        try {
            objectStore.getVolume(OzoneNames.ozoneVolumeName);
            return objectStore.getVolume(OzoneNames.ozoneVolumeName);
        } catch (OMException omException) {
            log.warn("Volume not created yet, generate new one");
            try {
                objectStore.createVolume(OzoneNames.ozoneVolumeName);
                return objectStore.getVolume(OzoneNames.ozoneVolumeName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Bean
    public OzoneBucket bucketJson(OzoneVolume volume) {
        try {
            return volume.getBucket(OzoneNames.ozoneJSONBucketName);
        } catch (OMException omException) {
            try {
                log.warn("Bucket for JSON not created yet, generate new one");
                volume.createBucket(OzoneNames.ozoneJSONBucketName);
                return volume.getBucket(OzoneNames.ozoneJSONBucketName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Bean
    public OzoneBucket bucketParquet(OzoneVolume volume) {
        try {
            return volume.getBucket(OzoneNames.ozoneBucketName);
        } catch (OMException omException) {
            try {
                log.warn("Bucket for Parquet not created yet, generate new one");
                volume.createBucket(OzoneNames.ozoneBucketName);
                return volume.getBucket(OzoneNames.ozoneBucketName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
            configuration.set("fs.defaultFS", "ofs://localhost:9874");
            configuration.setInt("ipc.maximum.data.length", 134217728);
            configuration.set("fs.ofs.impl", "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem");
            //configuration.set("ozone.client.stream.buffer.increment", "16KB");
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
