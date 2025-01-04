package one.terenin.datagenerator.configuration.property_holder;

import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.FieldDefaults;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource("classpath:/application.yaml")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
public class OzoneConfigurationPropertySource {

    final boolean defaultConfigLoadingEnable;
    final String omAddress;
    final boolean ozoneSecurityEnable;

    public OzoneConfigurationPropertySource(
            @Value("${ozone.default.conf.enable}") Boolean defaultConfigLoadingEnable,
            @Value("${ozone.om.address}") String omAddress,
            @Value("${ozone.security.enabled}") Boolean ozoneSecurityEnable
            ) {
        this.defaultConfigLoadingEnable = Boolean.TRUE.equals(defaultConfigLoadingEnable);
        this.omAddress = omAddress;
        this.ozoneSecurityEnable = Boolean.TRUE.equals(ozoneSecurityEnable);
    }

}
