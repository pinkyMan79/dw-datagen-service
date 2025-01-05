package one.terenin.datagenerator.generator.parent;

import one.terenin.datagenerator.dto.DataBundle;

import java.io.IOException;
import java.util.*;

public interface DataGenerator {

    void generate(int count) throws IOException;

    default List<DataBundle> generateSampleData(int count) {
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
}
