package one.terenin.datagenerator.generator;

import one.terenin.datagenerator.common.OzoneNames;
import one.terenin.datagenerator.common.ParquetSchemaHolder;
import one.terenin.datagenerator.dto.DataBundle;
import one.terenin.datagenerator.generator.fw.OzoneOutputFile;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class ParquetDataGenerator {

    private final OzoneClient ozoneClient;

    public ParquetDataGenerator(OzoneClient ozoneClient) {
        this.ozoneClient = ozoneClient;
    }

    public void generateAndWriteParquets(int count) throws IOException {
        String outputFileName = "data_bundle.parquet";
        List<DataBundle> data = generateFromBundle(count);
        Schema schema = ParquetSchemaHolder.asAvroSchema;

        // Get Ozone Volume and Bucket
        OzoneVolume volume = ozoneClient.getObjectStore().getVolume(OzoneNames.ozoneVolumeName);
        OzoneBucket bucket = volume.getBucket(OzoneNames.ozoneBucketName);

        // Create OzoneOutputStream for the file
        try (OzoneOutputStream ozoneOutputStream = bucket.createKey(outputFileName, 0L)) {
            // Create ParquetWriter that writes directly to the OzoneOutputStream, see fw.OzoneOutputFile
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                            new OzoneOutputFile(ozoneOutputStream))
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {

                for (DataBundle bundle : data) {
                    GenericRecord record = convertToAvroRecord(schema, bundle);
                    writer.write(record);
                }
            }
        }
    }

    private List<DataBundle> generateFromBundle(int count) {
        List<DataBundle> dataList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            dataList.add(DataBundle.builder()
                    .uuid(UUID.randomUUID().toString())
                    .name("Product " + i)
                    .description("Description of Product: lorem ipsum set a met..." + i)
                    .type("Type " + (i % 5))
                    .mainCategory("Category " + (i % 3))
                    .price(String.valueOf(100 + i))
                    .productOwner("Owner " + (i % 2))
                    .slaveCategories(Arrays.asList("Sub1", "Sub2", "Sub3"))
                    .options(Map.of("Key1", "Value1", "Key2", "Value2"))
                    .characteristics(Map.of("Feature1", "Spec1", "Feature2", "Spec2"))
                    .build());
        }
        return dataList;
    }

    private GenericRecord convertToAvroRecord(Schema schema, DataBundle bundle) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("uuid", bundle.getUuid());
        record.put("name", bundle.getName());
        record.put("description", bundle.getDescription());
        record.put("type", bundle.getType());
        record.put("mainCategory", bundle.getMainCategory());
        record.put("price", bundle.getPrice());
        record.put("productOwner", bundle.getProductOwner());
        record.put("slaveCategories", bundle.getSlaveCategories());
        record.put("options", bundle.getOptions());
        record.put("characteristics", bundle.getCharacteristics());
        return record;
    }
}