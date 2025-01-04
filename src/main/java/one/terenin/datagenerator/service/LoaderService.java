package one.terenin.datagenerator.service;

import lombok.AllArgsConstructor;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class LoaderService {

    private final OzoneClient client;

}
