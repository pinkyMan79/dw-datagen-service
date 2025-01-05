package one.terenin.datagenerator.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.AllArgsConstructor;
import one.terenin.datagenerator.common.OzoneNames;
import one.terenin.datagenerator.dto.DataBundle;
import one.terenin.datagenerator.generator.fw.OzoneOutputFile;
import one.terenin.datagenerator.generator.parent.DataGenerator;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

@Service
@AllArgsConstructor
public class JSONDataGenerator implements DataGenerator {

    private final OzoneClient client;

    @Override
    public void generate(int count) throws IOException {

        String keyName = "data_bundle.json";

        List<DataBundle> dataBundles = generateSampleData(10);

        OzoneVolume volume = client.getObjectStore().getVolume(OzoneNames.ozoneVolumeName);
        OzoneBucket bucket = volume.getBucket(OzoneNames.ozoneJSONBucketName);

        try (OzoneOutputStream outputStream = bucket.createKey(keyName, 0)) {
            writeJsonToFile(dataBundles, outputStream);
        }

    }

    private void writeJsonToFile(List<DataBundle> data, OutputStream stream) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
        writer.writeValue(stream, data);
    }
}
