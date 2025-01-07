package one.terenin.datagenerator.common;

import org.apache.avro.Schema;

public class ParquetSchemaHolder {

    public static String asJson = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"DataBundle\",\n" +
            "  \"namespace\": \"one.terenin.datagenerator\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"uuid\", \"type\": \"string\"},\n" +
            "    {\"name\": \"name\", \"type\": \"string\"},\n" +
            "    {\"name\": \"description\", \"type\": \"string\"},\n" +
            "    {\"name\": \"type\", \"type\": \"string\"},\n" +
            "    {\"name\": \"mainCategory\", \"type\": \"string\"},\n" +
            "    {\"name\": \"price\", \"type\": \"string\"},\n" +
            "    {\"name\": \"productOwner\", \"type\": \"string\"},\n" +
            "    {\"name\": \"slaveCategories\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n" +
            "    {\"name\": \"options\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},\n" +
            "    {\"name\": \"characteristics\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}\n" +
            "  ]\n" +
            "}";

    public static Schema asAvroSchema = new Schema.Parser().parse(asJson);


}
