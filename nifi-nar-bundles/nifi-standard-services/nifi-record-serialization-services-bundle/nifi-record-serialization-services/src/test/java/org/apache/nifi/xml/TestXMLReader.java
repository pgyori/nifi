/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.xml;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class TestXMLReader {

    private final String ATTRIBUTE_PREFIX = "attribute_prefix";
    private final String CONTENT_NAME = "content_field";
    private final String EVALUATE_IS_ARRAY = "xml.stream.is.array";

    private TestRunner setup(Map<PropertyDescriptor, String> xmlReaderProperties) throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(TestXMLReaderProcessor.class);
        XMLReader reader = new XMLReader();

        runner.addControllerService("xml_reader", reader);
        runner.setProperty(TestXMLReaderProcessor.XML_READER, "xml_reader");

        for (Map.Entry<PropertyDescriptor, String> entry : xmlReaderProperties.entrySet()) {
            runner.setProperty(reader, entry.getKey(), entry.getValue());
        }

        runner.enableControllerService(reader);
        return runner;
    }

    @Test
    public void testRecordFormatDeterminedBasedOnAttribute() throws IOException, InitializationException {
        // GIVEN
        String outputSchemaPath = "src/test/resources/xml/testschema";
        String outputSchemaText = new String(Files.readAllBytes(Paths.get(outputSchemaPath)));

        Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_EVALUATE.getValue());
        TestRunner runner = setup(xmlReaderProperties);

        // WHEN
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        runner.enqueue(is, Collections.singletonMap(EVALUATE_IS_ARRAY, "true"));
        runner.run();

        // THEN
        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS);
        List<String> records = Arrays.asList((new String(runner.getContentAsByteArray(flowFile.get(0)))).split("\n"));

        assertEquals(4, records.size());
    }

    @Test
    public void testRecordFormatArray() throws IOException, InitializationException {
        // GIVEN
        String outputSchemaPath = "src/test/resources/xml/testschema";
        String outputSchemaText = new String(Files.readAllBytes(Paths.get(outputSchemaPath)));

        Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_ARRAY.getValue());
        TestRunner runner = setup(xmlReaderProperties);

        // WHEN
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        runner.enqueue(is, Collections.singletonMap(EVALUATE_IS_ARRAY, "true"));
        runner.run();

        // THEN
        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS);
        List<String> records = Arrays.asList((new String(runner.getContentAsByteArray(flowFile.get(0)))).split("\n"));

        assertEquals(4, records.size());
    }

    @Test
    public void testRecordFormatNotArray() throws IOException, InitializationException {
        // GIVEN
        String outputSchemaPath = "src/test/resources/xml/testschema";
        String outputSchemaText = new String(Files.readAllBytes(Paths.get(outputSchemaPath)));

        Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_SINGLE.getValue());
        TestRunner runner = setup(xmlReaderProperties);

        // WHEN
        InputStream is = new FileInputStream("src/test/resources/xml/person.xml");
        runner.enqueue(is, Collections.singletonMap(EVALUATE_IS_ARRAY, "true"));
        runner.run();

        // THEN
        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS);
        List<String> records = Arrays.asList(new String(runner.getContentAsByteArray(flowFile.get(0))).split("\n"));

        assertEquals(1, records.size());
    }

    @Test
    public void testAttributePrefix() throws IOException, InitializationException {
        // GIVEN
        String outputSchemaPath = "src/test/resources/xml/testschema";
        String outputSchemaText = new String(Files.readAllBytes(Paths.get(outputSchemaPath)));

        Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        xmlReaderProperties.put(XMLReader.ATTRIBUTE_PREFIX, "${" + ATTRIBUTE_PREFIX + "}");
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_ARRAY.getValue());
        TestRunner runner = setup(xmlReaderProperties);

        // WHEN
        InputStream is = new FileInputStream("src/test/resources/xml/people.xml");
        runner.enqueue(is, Collections.singletonMap(ATTRIBUTE_PREFIX, "ATTR_"));
        runner.run();

        // THEN
        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS);
        List<String> records = Arrays.asList(new String(runner.getContentAsByteArray(flowFile.get(0))).split("\n"));

        assertEquals(4, records.size());
        assertEquals("MapRecord[{COUNTRY=USA, ATTR_ID=P1, NAME=Cleve Butler, AGE=42}]", records.get(0));
        assertEquals("MapRecord[{COUNTRY=UK, ATTR_ID=P2, NAME=Ainslie Fletcher, AGE=33}]", records.get(1));
        assertEquals("MapRecord[{COUNTRY=FR, ATTR_ID=P3, NAME=Amélie Bonfils, AGE=74}]", records.get(2));
        assertEquals("MapRecord[{COUNTRY=USA, ATTR_ID=P4, NAME=Elenora Scrivens, AGE=16}]", records.get(3));
    }

    @Test
    public void testContentField() throws IOException, InitializationException {
        // GIVEN
        String outputSchemaPath = "src/test/resources/xml/testschema2";
        String outputSchemaText = new String(Files.readAllBytes(Paths.get(outputSchemaPath)));

        Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        xmlReaderProperties.put(XMLReader.CONTENT_FIELD_NAME, "${" + CONTENT_NAME + "}");
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_ARRAY.getValue());
        TestRunner runner = setup(xmlReaderProperties);

        // WHEN
        InputStream is = new FileInputStream("src/test/resources/xml/people_tag_in_characters.xml");
        runner.enqueue(is, Collections.singletonMap(CONTENT_NAME, "CONTENT"));
        runner.run();

        // THEN
        List<MockFlowFile> flowFile = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS);
        List<String> records = Arrays.asList(new String(runner.getContentAsByteArray(flowFile.get(0))).split("\n"));

        assertEquals(5, records.size());
        assertEquals("MapRecord[{ID=P1, NAME=MapRecord[{CONTENT=Cleve Butler, ATTR=attr content, INNER=inner content}], AGE=42}]", records.get(0));
        assertEquals("MapRecord[{ID=P2, NAME=MapRecord[{CONTENT=Ainslie Fletcher, ATTR=attr content, INNER=inner content}], AGE=33}]", records.get(1));
        assertEquals("MapRecord[{ID=P3, NAME=MapRecord[{CONTENT=Amélie Bonfils, ATTR=attr content, INNER=inner content}], AGE=74}]", records.get(2));
        assertEquals("MapRecord[{ID=P4, NAME=MapRecord[{CONTENT=Elenora Scrivens, ATTR=attr content, INNER=inner content}], AGE=16}]", records.get(3));
        assertEquals("MapRecord[{ID=P5, NAME=MapRecord[{INNER=inner content}]}]", records.get(4));
    }

    @Test
    public void testInferSchema() throws InitializationException, IOException {
        // GIVEN
        String expectedContent = "MapRecord[{software=MapRecord[{" + CONTENT_NAME + "=Apache NiFi, favorite=true}], num=123, name=John Doe}]";

        Map<PropertyDescriptor, String> xmlReaderProperties = new HashMap<>();
        xmlReaderProperties.put(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA.getValue());
        xmlReaderProperties.put(XMLReader.RECORD_FORMAT, XMLReader.RECORD_SINGLE.getValue());
        xmlReaderProperties.put(XMLReader.CONTENT_FIELD_NAME, CONTENT_NAME);
        TestRunner runner = setup(xmlReaderProperties);

        // WHEN
        InputStream is = new FileInputStream("src/test/resources/xml/person_record.xml");
        runner.enqueue(is);
        runner.run();

        // THEN
        MockFlowFile out = runner.getFlowFilesForRelationship(TestXMLReaderProcessor.SUCCESS).get(0);
        String actualContent = out.getContent();
        assertEquals(expectedContent, actualContent);
    }
}
