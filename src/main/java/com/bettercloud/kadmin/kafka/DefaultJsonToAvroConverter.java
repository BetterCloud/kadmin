package com.bettercloud.kadmin.kafka;

import com.bettercloud.kadmin.api.kafka.AvrifyConverter;
import com.bettercloud.kadmin.api.kafka.JsonToAvroConverter;
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

/**
 * Created by davidesposito on 7/6/16.
 */
@Component
public class DefaultJsonToAvroConverter implements JsonToAvroConverter, AvrifyConverter {

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

    @Override
    public String avrify(String json, String schema) {
        return json;
    }
}
