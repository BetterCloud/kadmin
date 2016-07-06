package com.bettercloud.kadmin.kafka;

import com.bettercloud.kadmin.api.kafka.JsonToAvroConverter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.springframework.stereotype.Component;

import java.io.*;

/**
 * Created by davidesposito on 7/6/16.
 */
@Component
public class DefaultJsonToAvroConverter implements JsonToAvroConverter {

    @Override
    public Object convert(String json, String schemaStr) {
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

        return datum;
    }
}
