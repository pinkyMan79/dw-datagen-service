package one.terenin.datagenerator.generator;

import lombok.RequiredArgsConstructor;
import one.terenin.datagenerator.common.OzoneNames;
import one.terenin.datagenerator.common.ParquetSchemaHolder;
import one.terenin.datagenerator.dto.DataBundle;
import one.terenin.datagenerator.generator.fw.BufferedWriter;
import one.terenin.datagenerator.generator.fw.OzoneOutputFile;
import one.terenin.datagenerator.generator.parent.DataGenerator;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

@Service
@RequiredArgsConstructor
public class ParquetDataGenerator implements DataGenerator<byte[]> {

    @Override
    public void generate(int count, OzoneClient client) throws IOException {
        String outputFileName = "data_bundle.parquet";
        List<DataBundle> data = generateSampleData(count);
        Schema schema = ParquetSchemaHolder.asAvroSchema;

        OzoneVolume volume = client.getObjectStore().getVolume(OzoneNames.ozoneVolumeName);
        OzoneBucket bucket = volume.getBucket(OzoneNames.ozoneBucketName);

        try (OzoneOutputStream ozoneOutputStream = bucket.createKey(outputFileName, 0L)) {
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

    @Override
    public byte[] generateWithResponse(int count) throws IOException {
        String outputFileName = "data_bundle.parquet";
        List<DataBundle> data = generateSampleData(count);
        Schema schema = ParquetSchemaHolder.asAvroSchema;

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                new BufferedWriter(outputStream))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()){
            for (DataBundle bundle : data) {
                GenericRecord record = convertToAvroRecord(schema, bundle);
                writer.write(record);
            }
        }
        return outputStream.toByteArray();
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