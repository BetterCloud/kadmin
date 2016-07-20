package com.bettercloud.kadmin.kafka;

import com.bettercloud.kadmin.api.kafka.AvrifyConverter;
import com.bettercloud.kadmin.api.kafka.JsonToAvroConverter;
import com.bettercloud.util.LoggerUtils;
import com.bettercloud.util.Opt;
import com.bettercloud.util.StreamUtils;
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
import org.slf4j.Logger;
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
    private static final Logger LOGGER = LoggerUtils.get(DefaultJsonToAvroConverter.class);

    @Override
    public Object convert(String json, String schemaStr) {
        LOGGER.debug("Converting: {}", json.replaceAll("\n", " "));
        json = avrify(json, schemaStr);
        LOGGER.debug("Avrified to: {}", json.replaceAll("\n", " "));

        InputStream input = new ByteArrayInputStream(json.getBytes());
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
        LOGGER.debug("Final Model: {} - {}", datum.getClass().getSimpleName(), datum);
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
            Schema recSchema = isUnion(schema) ? getRecordSchema(message, schema) : schema;
            ObjectNode oMessage = (ObjectNode) message;
            // recursion before union mapping
            Map<String, Schema.Field> fieldMap = getFieldMap(recSchema);
            message.fields().forEachRemaining(e -> {
                Schema.Field field = fieldMap.get(e.getKey());
                Schema fs = field.schema();
                JsonNode newField = avrify(e.getValue(), fs);
                oMessage.replace(e.getKey(), newField);
            });
        } else if (message.isObject() && isMap(schema, true)) {
            Schema recSchema = isUnion(schema) ? getMapSchema(message, schema) : schema;
            ObjectNode oMessage = (ObjectNode) message;
            // recursion before union mapping
            Schema valueType = recSchema.getValueType();
            message.fields().forEachRemaining(e -> {
                JsonNode newField = avrify(e.getValue(), valueType);
                oMessage.replace(e.getKey(), newField);
            });
        } else if (message.isArray() && isArray(schema, true)) {
            Schema arrSchema = isUnion(schema) ? getArraySchema(message, schema) : schema;
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
                if (unionTypes.contains(Schema.Type.ENUM)) {
                    String val = message.asText();
                    List<Schema> validEnumSchemas = schema.getTypes().stream()
                            .filter(s -> isEnum(s, false))
                            .filter(s -> s.getEnumSymbols().contains(val))
                            .collect(Collectors.toList());
                    switch (validEnumSchemas.size()) {
                        case 0:
                            break;
                        case 1:
                            mappedObject.replace(validEnumSchemas.get(0).getFullName(), message);
                        default:
                            // TODO: matched multiple enums, what to do?
                            LOGGER.warn("Matched multiple enums: {} => {}",
                                    message.asText(),
                                    validEnumSchemas.stream()
                                            .map(s -> s.getName())
                                            .collect(Collectors.toList()));
                            break;
                    }
                } else if (unionTypes.contains(Schema.Type.STRING)) {
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
                    Opt.of(getRecordSchema(message, schema))
                            .ifPresent(recSchema -> mappedObject.replace(recSchema.getFullName(), message));
                }
                if (unionTypes.contains(Schema.Type.MAP)) {
                    mappedObject.replace("map", message);
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

    private boolean isMap(Schema schema, boolean allowUnion) {
        return isType(schema, allowUnion, Schema.Type.MAP);
    }

    private boolean isNullable(Schema schema, boolean allowUnion) {
        return isType(schema, allowUnion, Schema.Type.NULL);
    }

    private boolean isEnum(Schema schema, boolean allowUnion) {
        return isType(schema, allowUnion, Schema.Type.ENUM);
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

    public Schema getRecordSchema(JsonNode message, Schema unionSchema) {
        Schema recSchema = null;
        List<Schema> records = unionSchema.getTypes().stream()
                .filter(t -> isRecord(t, false))
                .collect(Collectors.toList());
        if (records.size() == 1) {
            recSchema = records.get(0);
        } else {
            List<Schema> matchedSchemas = records.stream()
                    .filter(r -> checkSchemaMatch(message, r))
                    .collect(Collectors.toList());
            recSchema = getSingleFromList(matchedSchemas);
        }
        return recSchema;
    }

    public Schema getMapSchema(JsonNode message, Schema unionSchema) {
        List<Schema> maps = unionSchema.getTypes().stream()
                .filter(t -> isMap(t, false))
                .collect(Collectors.toList());
        return getSingleFromList(maps);
    }

    public Schema getArraySchema(JsonNode message, Schema unionSchema) {
        List<Schema> arraySchemas = unionSchema.getTypes().stream()
                .filter(t -> isArray(t, false))
                .collect(Collectors.toList());
        return getSingleFromList(arraySchemas);
    }

    private Schema getSingleFromList(List<Schema> schemas) {
        switch (schemas.size()) {
            case 0:
                throw new RuntimeException("Could not find any matching schemas");
            case 1:
                return schemas.get(0);
            default:
                throw new RuntimeException("Found multiple matching schemas");
        }
    }

    public Schema guessSchema(JsonNode message, List<Schema> validSchemas) {
        Map<Schema.Type, List<Schema>> typeMap = validSchemas.stream()
                .collect(Collectors.groupingBy(s -> s.getType()));
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
                if (bestGuess == null && typeMap.containsKey(Schema.Type.ENUM)) {
                    List<Schema> validEnumSchemas = guessEnumType(message.asText(), typeMap.get(Schema.Type.ENUM));
                    switch (validEnumSchemas.size()) {
                        case 0:
                            break;
                        case 1:
                            bestGuess = validEnumSchemas.get(0);
                        default:
                            // TODO: matched multiple enums, what to do?
                            LOGGER.warn("Matched multiple enums: {} => {}",
                                    message.asText(),
                                    validEnumSchemas.stream()
                                            .map(s -> s.getName())
                                            .collect(Collectors.toList()));
                            break;
                    }
                }
                if (bestGuess == null && typeMap.containsKey(Schema.Type.STRING)) {
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
                    List<Schema> matchedSchemas = typeMap.get(Schema.Type.RECORD).stream()
                            .filter(s -> checkSchemaMatch(message, s))
                            .collect(Collectors.toList());
                    switch (matchedSchemas.size()) {
                        case 0:
                            throw new RuntimeException("Could not find matching schema");
                        case 1:
                            bestGuess = matchedSchemas.get(0);
                            break;
                        default:
                            throw new RuntimeException("Found multiple matching schemas");
                    }
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

    public List<Schema> guessEnumType(String val, List<Schema> validEnumSchemas) {
        return validEnumSchemas.stream()
                .filter(s -> isEnum(s, false))
                .filter(s -> s.getEnumSymbols().contains(val))
                .collect(Collectors.toList());
    }

    public boolean checkSchemaMatch(JsonNode message, Schema recordSchema) {
        List<Schema.Field> fields = recordSchema.getFields();
        Set<String> schemaFieldNameSet = fields.stream()
                .map(f -> f.name())
                .collect(Collectors.toSet());
        long extraSchemaFields = fields.stream()
                .filter(f -> !fieldIsValid(message, f))
                .count();
        long extraMessageFields = StreamUtils.asStream(message.fieldNames())
                .filter(n -> !schemaFieldNameSet.contains(n))
                .count();
        return extraSchemaFields == 0 && extraMessageFields == 0;
    }

    public boolean fieldIsValid(JsonNode message, Schema.Field f) {
        String name = f.name();
        return message.has(name) || isNullable(f.schema(), true);
    }
}
