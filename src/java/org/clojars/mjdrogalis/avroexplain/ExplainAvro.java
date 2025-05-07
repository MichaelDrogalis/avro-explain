package org.clojars.mjdrogalis.avroexplain;

import java.util.List;
import java.util.Map;
import java.util.Set;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

import org.apache.avro.Schema;

import org.clojars.mjdrogalis.avroexplain.Explanation;

public class ExplainAvro {

    private static final IFn require;
    private static final IFn explain;

    static {
        require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("io.mdrogalis.avro-explain.core"));

        explain = Clojure.var("io.mdrogalis.avro-explain.core", "explain");
    }

    public static Explanation explain(Schema schema, String data) {
        return (Explanation) explain.invoke(schema, data);
    }

    public static Explanation explain(Schema schema, int data) {
        return (Explanation) explain.invoke(schema, data);
    }

    public static Explanation explain(Schema schema, long data) {
        return (Explanation) explain.invoke(schema, data);
    }

    public static Explanation explain(Schema schema, float data) {
        return (Explanation) explain.invoke(schema, data);
    }

    public static Explanation explain(Schema schema, double data) {
        return (Explanation) explain.invoke(schema, data);
    }

    public static Explanation explain(Schema schema, boolean data) {
        return (Explanation) explain.invoke(schema, data);
    }

    public static Explanation explain(Schema schema, byte data) {
        return (Explanation) explain.invoke(schema, data);
    }

    public static Explanation explain(Schema schema, short data) {
        return (Explanation) explain.invoke(schema, data);
    }

    public static Explanation explain(Schema schema, char data) {
        return (Explanation) explain.invoke(schema, data);
    }

    public static Explanation explain(Schema schema, Map<?, ?> data) {
        return (Explanation) explain.invoke(schema, data);
    }

    public static Explanation explain(Schema schema, List<?> data) {
        return (Explanation) explain.invoke(schema, data);
    }

    public static Explanation explain(Schema schema, Set<?> data) {
        return (Explanation) explain.invoke(schema, data);
    }

}
