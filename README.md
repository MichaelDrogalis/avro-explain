# avro-explain

A Clojure library designed to ... well, that part is up to you.

## Usage

```java
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import io.mdrogalis.avroexplain.ExplainAvro;
import io.mdrogalis.avroexplain.Explanation;

import java.util.Map;
import java.util.HashMap;

public class Runner {

    public static void main(String[] args) {
        Schema schema = SchemaBuilder
            .record("PageView")
            .namespace("com.example.analytics")
            .fields()
                .name("userId").type().stringType().noDefault()
                .name("url").type().stringType().noDefault()
                .name("durationSeconds")
                    .type().unionOf().nullType().and().floatType().endUnion()
                    .nullDefault()
            .endRecord();

        Map<String, Object> data = new HashMap<>();
        data.put("userId", "user-123");
        data.put("url", "https://example.com/home");
        data.put("durationSeconds", 12.5f);

        Explanation exp = ExplainAvro.explain(schema, data);

        System.out.println("Reason: " + exp.getReason());
        System.out.println("Root cause: " + exp.getRootCause());

        System.out.println("Sub schema: " + exp.getSubSchema());
        System.out.println("Schema path: " + exp.getSchemaPath());

        System.out.println("Sub data: " + exp.getSubData());
        System.out.println("Data path: " + exp.getDataPath());
    }

}
```

```
Reason: missing-union-hint
Root cause: your schema included a union, but your data didn't include a required hint for its concrete type. This StackOverflow thread describes how to provide the type hint: https://stackoverflow.com/q/27485580
Sub schema: [null, float]
Schema path: [fields, 2, type]
Sub data: 12.5
Data path: [durationSeconds]
```

## License

Copyright © 2025 Michael Drogalis

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
