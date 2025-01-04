package one.terenin.datagenerator.configuration.property;

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

    public OzoneConfigurationPropertySource(@Value("${ozone.default.conf.enable}") Boolean defaultConfigLoadingEnable, boolean defaultConfigLoadingEnable1) {
        this.defaultConfigLoadingEnable = Boolean.TRUE.equals(defaultConfigLoadingEnable);
    }

}
