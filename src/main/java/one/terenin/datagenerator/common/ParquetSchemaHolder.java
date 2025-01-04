package one.terenin.datagenerator.common;

import org.apache.avro.Schema;

public class ParquetSchemaHolder {

    public static String asJson = """
                {
                  "type": "record",
                  "name": "DataBundle",
                  "namespace": "one.terenin.datagenerator",
                  "fields": [
                    {"name": "uuid", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "description", "type": "string"},
                    {"name": "type", "type": "string"},
                    {"name": "mainCategory", "type": "string"},
                    {"name": "price", "type": "string"},
                    {"name": "productOwner", "type": "string"},
                    {"name": "slaveCategories", "type": {"type": "array", "items": "string"}},
                    {"name": "options", "type": {"type": "map", "values": "string"}},
                    {"name": "characteristics", "type": {"type": "map", "values": "string"}}
                  ]
                }
                """;

    public static Schema asAvroSchema = new Schema.Parser().parse(asJson);


}
