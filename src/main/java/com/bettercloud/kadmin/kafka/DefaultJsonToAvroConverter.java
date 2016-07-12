package com.bettercloud.kadmin.kafka;

import com.bettercloud.kadmin.api.kafka.AvrifyConverter;
import com.bettercloud.kadmin.api.kafka.JsonToAvroConverter;
import com.bettercloud.util.Opt;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by davidesposito on 7/6/16.
 */
@Component
public class DefaultJsonToAvroConverter implements JsonToAvroConverter, AvrifyConverter {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Object convert(String json, String schemaStr) {
        String avroJson = avrify(json, schemaStr);

        InputStream input = new ByteArrayInputStream(avroJson.getBytes());
        DataInputStream din = new DataInputStream(input);

        Schema schema = new Schema.Parser().parse(schemaStr);

        Object datum = null;
        try {
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            DatumReader<Object> reader = new GenericDatumReader<>(schema);
            datum = reader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return datum;
    }

    @Override
    public String avrify(String rawJson, String rawSchema) {
        String converted = rawJson;
        try {
            JsonNode json = mapper.readTree(rawJson);
            if (json.isObject()) {
                Schema schema = new Schema.Parser().parse(rawSchema);
                converted = Opt.of(avrify(json, schema))
                        .map(n -> n.toString())
                        .orElse(rawJson);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return converted;
    }

    private JsonNode avrify(JsonNode message, Schema schema) {
        if (message.isObject() && isRecord(schema, true)) {
            Schema recSchema = isUnion(schema) ? getRecordSchema(schema) : schema;
            ObjectNode oMessage = (ObjectNode) message;
            // recursion before union mapping
            Map<String, Schema.Field> fieldMap = getFieldMap(recSchema);
            message.fields().forEachRemaining(e -> {
                Schema.Field field = fieldMap.get(e.getKey());
                Schema fs = field.schema();
                JsonNode newField = avrify(e.getValue(), fs);
                oMessage.replace(e.getKey(), newField);
            });
        } else if (message.isArray() && isArray(schema, true)) {
            Schema arrSchema = isUnion(schema) ? getArraySchema(schema) : schema;
            arrSchema = arrSchema.getElementType();
            List<Schema> validSchemas = isUnion(arrSchema) ? arrSchema.getTypes() : Lists.newArrayList(arrSchema);
            ArrayNode tempArrNode = mapper.createArrayNode();
            message.elements().forEachRemaining(ele -> {
                Schema fs = guessSchema(ele, validSchemas);
                JsonNode newField = avrify(ele, fs);
                tempArrNode.add(newField);
            });
            message = tempArrNode;
        }
        // union mapping after recusion
        if (isUnion(schema)) {
            message = convertUnion(message, schema);
        }
        return message;
    }

    private JsonNode convertUnion(JsonNode message, Schema schema) {
        Set<Schema.Type> unionTypes = schema.getTypes().stream()
                .map(s -> s.getType())
                .collect(Collectors.toSet());
        ObjectNode mappedObject = mapper.createObjectNode();
        JsonNode mapped = mappedObject;
        switch (message.getNodeType()) {
            case BOOLEAN:
                if (unionTypes.contains(Schema.Type.BOOLEAN)) {
                    mappedObject.put("boolean", message.asBoolean());
                }
                break;
            case STRING:
                if (unionTypes.contains(Schema.Type.STRING)) {
                    mappedObject.put("string", message.asText());
                }
                break;
            case NUMBER:
                if (message.isInt() && unionTypes.contains(Schema.Type.INT)) {
                    mappedObject.put("int", message.asInt());
                } else if (message.isLong() && unionTypes.contains(Schema.Type.LONG)) {
                    mappedObject.put("long", message.asLong());
                } else if (message.isDouble() && unionTypes.contains(Schema.Type.DOUBLE)
                        || message.isInt() && unionTypes.contains(Schema.Type.FLOAT)) {
                    mappedObject.put("float", message.asDouble());
                }
                break;
            case OBJECT:
                if (unionTypes.contains(Schema.Type.RECORD)) {
                    Opt.of(getRecordSchema(schema))
                        .ifPresent(recSchema -> mappedObject.replace(recSchema.getFullName(), message));
                }
                break;
            case POJO:
                break;
            case ARRAY:
                if (unionTypes.contains(Schema.Type.ARRAY)) {
                    mappedObject.replace("array", message);
                }
                break;
            case NULL:
                if (unionTypes.contains(Schema.Type.NULL)) {
                    mapped = null;
                }
                break;
            case BINARY:
            case MISSING:
                break;
        }
        return mapped;
    }

    private Map<String, Schema.Field> getFieldMap(Schema schema) {
        return schema.getFields().stream()
                .collect(Collectors.toMap(
                        f -> f.name(),
                        f -> f
                ));
    }

    private boolean isUnion(Schema schema) {
        return schema.getType().equals(Schema.Type.UNION);
    }

    private boolean isRecord(Schema schema, boolean allowUnion) {
        return isType(schema, allowUnion, Schema.Type.RECORD);
    }

    private boolean isArray(Schema schema, boolean allowUnion) {
        return isType(schema, allowUnion, Schema.Type.ARRAY);
    }

    private boolean isType(Schema schema, boolean allowUnion, Schema.Type type) {
        if (isUnion(schema) && allowUnion) {
            return schema.getTypes().stream()
                    .map(s -> s.getType())
                    .collect(Collectors.toList())
                    .contains(type);
        }
        return schema.getType().equals(type);
    }

    public Schema getRecordSchema(Schema unionSchema) {
        Schema recSchema = null;
        List<Schema> records = unionSchema.getTypes().stream()
                .filter(t -> isRecord(t, false))
                .collect(Collectors.toList());
        if (records.size() == 1) {
            recSchema = records.get(0);
        } else {
            // uhhhhh... wut now?
        }
        return recSchema;
    }

    public Schema getArraySchema(Schema unionSchema) {
        Schema arrSchema = null;
        List<Schema> records = unionSchema.getTypes().stream()
                .filter(t -> isArray(t, false))
                .collect(Collectors.toList());
        if (records.size() == 1) {
            arrSchema = records.get(0);
        } else {
            // uhhhhh... wut now?
        }
        return arrSchema;
    }

    public Schema guessSchema(JsonNode message, List<Schema> validSchemas) {
        Map<Schema.Type, List<Schema>> typeMap = validSchemas.stream()
                .collect(Collectors.groupingBy(s -> s.getType()));
        typeMap.entrySet().stream().forEach(e -> {
            if (e.getValue().size() > 1) {
                System.err.println("Found duplicate schemas for type: " + e.getKey());
            }
        });
        Schema bestGuess = null;
        switch (message.getNodeType()) {
            case BOOLEAN:
                if (typeMap.containsKey(Schema.Type.BOOLEAN)) {
                    bestGuess = typeMap.get(Schema.Type.BOOLEAN).get(0);
                }
                break;
            case STRING:
                if (typeMap.containsKey(Schema.Type.BOOLEAN)) {
                    bestGuess = typeMap.get(Schema.Type.BOOLEAN).get(0);
                }
                if (typeMap.containsKey(Schema.Type.STRING)) {
                    bestGuess = typeMap.get(Schema.Type.STRING).get(0);
                }
                break;
            case NUMBER:
                if (message.isInt() && typeMap.containsKey(Schema.Type.INT)) {
                    bestGuess = typeMap.get(Schema.Type.INT).get(0);
                } else if (message.isLong() && typeMap.containsKey(Schema.Type.LONG)) {
                    bestGuess = typeMap.get(Schema.Type.LONG).get(0);
                } else if (message.isDouble() && typeMap.containsKey(Schema.Type.DOUBLE)) {
                    bestGuess = typeMap.get(Schema.Type.DOUBLE).get(0);
                } else if (message.isFloat() && typeMap.containsKey(Schema.Type.FLOAT)) {
                    bestGuess = typeMap.get(Schema.Type.FLOAT).get(0);
                }
                break;
            case OBJECT:
                if (typeMap.containsKey(Schema.Type.RECORD)) {
                    bestGuess = typeMap.get(Schema.Type.RECORD).get(0);
                }
                break;
            case POJO:
                break;
            case ARRAY:
                if (typeMap.containsKey(Schema.Type.ARRAY)) {
                    bestGuess = typeMap.get(Schema.Type.ARRAY).get(0);
                }
                break;
            case NULL:
                if (typeMap.containsKey(Schema.Type.NULL)) {
                    bestGuess = typeMap.get(Schema.Type.NULL).get(0);
                }
                break;
            case BINARY:
            case MISSING:
                break;
        }
        return bestGuess;
    }

//    public GenericRecord avrify(String rawJson, String rawSchema) {
//        GenericRecord record = null;
//        try {
//            JsonNode json = mapper.readTree(rawJson);
//            if (json.isObject()) {
//                Schema schema = new Schema.Parser().parse(rawSchema);
//                final GenericRecord tempRecord = new GenericData.Record(schema);
//                json.fields().forEachRemaining(n -> {
//                    tempRecord.put(n.getKey(), n.getValue());
//                });
//                record = tempRecord;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return record;
//    }
}
