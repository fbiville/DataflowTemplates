package com.google.cloud.teleport.v2.neo4j.templates;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.storage.conditions.GCSArtifactsCheck;
import org.apache.beam.it.neo4j.Neo4jResourceManager;
import org.apache.beam.it.neo4j.conditions.Neo4jQueryCheck;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(GoogleCloudToNeo4j.class)
public class DeadletterIT extends TemplateTestBase {

    private Neo4jResourceManager neo4jClient;

    @Before
    public void setup() {
        neo4jClient =
                Neo4jResourceManager.builder(testName)
                        .setAdminPassword("letmein!")
                        .setHost(TestProperties.hostIp())
                        .build();
    }

    @After
    public void tearDown() {
        ResourceManagerUtils.cleanResources(neo4jClient);
    }

    @Test
    public void failed_writes_are_stored_in_configured_buckets() throws IOException {
        gcsClient.createArtifact("spec.json", "{\n" +
                                              "  \"source\": {\n" +
                                              "    \"type\": \"text\",\n" +
                                              "    \"format\": \"EXCEL\",\n" +
                                              "    \"name\": \"zeroes\",\n" +
                                              "    \"data\": [\n" +
                                              "      [0],\n" +
                                              "      [0],\n" +
                                              "      [0],\n" +
                                              "      [0]\n" +
                                              "    ],\n" +
                                              "    \"ordered_field_names\": \"value\"\n" +
                                              "  },\n" +
                                              "  \"targets\": [\n" +
                                              "    {\n" +
                                              "      \"custom_query\": {\n" +
                                              "        \"name\": \"oopsie\",\n" +
                                              "        \"source\": \"zeroes\",\n" +
                                              "        \"query\": \"UNWIND $rows AS row CREATE (:DivisionByZero {result: 1/toInteger(row.value)})\"\n" +
                                              "      }\n" +
                                              "    }\n" +
                                              "  ]\n" +
                                              "}\n");
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
                        // deadletterGcsPath likely a better name
                        .addParameter(
                                "deadLetterBucket", getGcsPath(testName) + "/deadletter.json");
        LaunchInfo info = launchTemplate(options);

        assertThatPipeline(info).isRunning();
        assertThatResult(
                pipelineOperator()
                        .waitForConditionAndCancel(
                                createConfig(info),
                                GCSArtifactsCheck.builder(gcsClient, testName, Pattern.compile(".*\\.json"))
                                        .setMinSize(1)
                                        .setMaxSize(1)
                                        .setArtifactContentMatcher("{\"errorMessage\":\"/ by zero\",\"sourceType\":\"TEXT_INLINE\",\"targetType\":\"QUERY\",\"query\":\"UNWIND $rows AS row CREATE (:DivisionByZero {result: 1/toInteger(row.value)})\",\"parameters\":{\"rows\":[{\"value\":\"0\"},{\"value\":\"0\"},{\"value\":\"0\"},{\"value\":\"0\"}]}}")
                                        .build()))
                .meetsConditions();
    }
}
