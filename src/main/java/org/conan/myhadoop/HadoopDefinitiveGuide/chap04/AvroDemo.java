package org.conan.myhadoop.HadoopDefinitiveGuide.chap04;

import com.google.protobuf.GeneratedMessage;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Created by zhangzhibo on 17-5-23.
 */
public class AvroDemo {
    public static void main(String[] args) throws Exception {
        StringPair datum = new StringPair();
        datum.left = "L";
        datum.right = "R";

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<StringPair> writer = new SpecificDatumWriter<StringPair>(StringPair.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();
        DatumReader<StringPair> reader = new SpecificDatumReader<StringPair>(StringPair.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(),null);
        StringPair resault = reader.read(null,decoder);
        System.out.println(resault.left.toString());
        System.out.println(resault.right.toString());


        File file = new File("data.avro");
        DatumWriter<GenericRecord> writer1 = new GenericDatumWriter<GenericRecord>(datum.getSchema());
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer1);
        dataFileWriter.create(datum.getSchema(),file);
        dataFileWriter.append(datum);
        dataFileWriter.close();
        DatumReader<GenericRecord> reader1 = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader= new DataFileReader<GenericRecord>(file,reader1);
        assertThat("Schem is the same",datum.getSchema(),is(dataFileReader.getSchema()));


    }
}
