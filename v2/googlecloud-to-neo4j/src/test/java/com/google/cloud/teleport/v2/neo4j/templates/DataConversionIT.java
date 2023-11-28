/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.neo4j.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.neo4j.Neo4jResourceManager;
import org.apache.beam.it.neo4j.conditions.Neo4jQueryCheck;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(GoogleCloudToNeo4j.class)
@RunWith(JUnit4.class)
public class DataConversionIT extends TemplateTestBase {

  private BigQueryResourceManager bigQueryClient;
  private Neo4jResourceManager neo4jClient;

  @Before
  public void setUp() {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    neo4jClient =
        Neo4jResourceManager.builder(testName)
            .setAdminPassword("letmein!")
            .setHost(TestProperties.hostIp())
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryClient, neo4jClient);
  }

  // NOTE: BIGNUMERIC, GEOGRAPHY, JSON and INTERVAL BigQuery column types are not supported by Beam
  @Test
  public void supportBigQueryDataTypes() throws Exception {
    TableId table =
        bigQueryClient.createTable(
            testName,
            Schema.of(
                Field.newBuilder("bool", StandardSQLTypeName.BOOL).build(),
                Field.newBuilder("int64", StandardSQLTypeName.INT64).build(),
                Field.newBuilder("float64", StandardSQLTypeName.FLOAT64).build(),
                Field.newBuilder("numeric", StandardSQLTypeName.NUMERIC).build(),
                Field.newBuilder("string", StandardSQLTypeName.STRING).build(),
                Field.newBuilder("bytes", StandardSQLTypeName.BYTES).build(),
                Field.newBuilder("timestamp", StandardSQLTypeName.TIMESTAMP).build(),
                Field.newBuilder("time", StandardSQLTypeName.TIME).build(),
                Field.newBuilder("datetime", StandardSQLTypeName.DATETIME).build()));
    Map<String, Object> sourceRow = new HashMap<>();
    sourceRow.put("bool", true);
    sourceRow.put("int64", 10L);
    sourceRow.put("float64", 40D);
    sourceRow.put("numeric", 50D);
    sourceRow.put("string", "string");
    sourceRow.put(
        "bytes",
        Base64.getEncoder().encodeToString(new byte[] {(byte) 0x89, (byte) 0x65, (byte) 0x89}));
    sourceRow.put("timestamp", "2023-12-15T12:13:14.156Z");
    sourceRow.put("time", "12:13:14");
    sourceRow.put("datetime", "2019-02-17 11:24:00.000");
    bigQueryClient.write(testName, List.of(RowToInsert.of(sourceRow)));
    gcsClient.createArtifact("spec.json", contentOf("/testing-specs/data-conversion/bq-spec.json"));
    gcsClient.createArtifact(
        "neo4j.json",
        String.format(
            "{\n"
                + "  \"server_url\": \"%s\",\n"
                + "  \"database\": \"%s\",\n"
                + "  \"auth_type\": \"basic\",\n"
                + "  \"username\": \"neo4j\",\n"
                + "  \"pwd\": \"%s\"\n"
                + "}",
            neo4jClient.getUri(), neo4jClient.getDatabaseName(), neo4jClient.getAdminPassword()));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("spec.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j.json"))
            .addParameter(
                "optionsJson", String.format("{\"bqtable\": \"%s\"}", toTableSpecStandard(table)));
    LaunchInfo info = launchTemplate(options);

    Map<String, Object> expectedRow = new HashMap<>();
    expectedRow.put("bool", true);
    expectedRow.put("int64", 10L);
    expectedRow.put("float64", 40D);
    expectedRow.put("numeric", 50D);
    expectedRow.put("string", "string");
    expectedRow.put("bytes", new byte[] {(byte) 0x89, (byte) 0x65, (byte) 0x89});
    expectedRow.put(
        "timestamp", ZonedDateTime.of(2023, 12, 15, 12, 13, 14, 156_000_000, ZoneId.of("UTC")));
    expectedRow.put("time", LocalTime.of(12, 13, 14));
    expectedRow.put("datetime", LocalDateTime.of(2019, 2, 17, 11, 24, 0, 0));
    assertThatPipeline(info).isRunning();
    assertThatResult(
            pipelineOperator()
                .waitForConditionAndCancel(
                    createConfig(info),
                    Neo4jQueryCheck.builder(neo4jClient)
                        .setQuery(
                            "MATCH (n) RETURN labels(n) AS labels, properties(n) AS props ORDER BY n.id ASC")
                        .setExpectedResult(
                            List.of(Map.of("labels", List.of("Node"), "props", expectedRow)))
                        .build()))
        .meetsConditions();
  }

  private String contentOf(String resourcePath) throws IOException {
    try (BufferedReader bufferedReader =
        new BufferedReader(
            new InputStreamReader(this.getClass().getResourceAsStream(resourcePath)))) {
      return bufferedReader.lines().collect(Collectors.joining("\n"));
    }
  }
}
