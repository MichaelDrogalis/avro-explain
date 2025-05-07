package org.clojars.mjdrogalis.avroexplain;

import java.util.List;
import java.util.Map;

public class Explanation {

    private final String rootCause;

    private final Object subSchema;

    private final List<?> schemaPath;

    private final Object subData;

    private final List<?> dataPath;

    private final String reason;

    private final Map<?, ?> errorData;

    public Explanation(String rootCause, Object subSchema, List<?> schemaPath, Object subData, List<?> dataPath, String reason, Map<?, ?> errorData) {
        this.rootCause = rootCause;
        this.subSchema = subSchema;
        this.schemaPath = schemaPath;
        this.subData = subData;
        this.dataPath = dataPath;
        this.reason = reason;
        this.errorData = errorData;
    }

    public String getRootCause() {
        return rootCause;
    }

    public Object getSubSchema() {
        return subSchema;
    }

    public List<?> getSchemaPath() {
        return schemaPath;
    }

    public Object getSubData() {
        return subData;
    }

    public List<?> getDataPath() {
        return dataPath;
    }

    public String getReason() {
        return reason;
    }

    public Map<?, ?> getErrorData() {
        return errorData;
    }

}
