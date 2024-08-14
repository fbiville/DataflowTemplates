/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.neo4j.telemetry.ReportedSourceType;
import org.neo4j.importer.v1.targets.TargetType;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public final class CypherWriteFailure implements Serializable {

    private final String errorMessage;
    private final ReportedSourceType sourceType;
    private final TargetType targetType;
    private final String query;
    private final Map<String, Object> parameters;

    public CypherWriteFailure(
            String errorMessage,
            ReportedSourceType sourceType,
            TargetType targetType,
            String query,
            Map<String, Object> parameters) {
        this.errorMessage = errorMessage;
        this.sourceType = sourceType;
        this.targetType = targetType;
        this.query = query;
        this.parameters = deepCopy(parameters);
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public ReportedSourceType getSourceType() {
        return sourceType;
    }

    public TargetType getTargetType() {
        return targetType;
    }

    public String getQuery() {
        return query;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CypherWriteFailure)) {
            return false;
        }

        CypherWriteFailure that = (CypherWriteFailure) o;
        return Objects.equals(errorMessage, that.errorMessage)
               && sourceType == that.sourceType
               && targetType == that.targetType
               && Objects.equals(query, that.query)
               && Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(errorMessage, sourceType, targetType, query, parameters);
    }

    @Override
    public String toString() {
        return "CypherWriteFailure{" +
               "errorMessage='" + errorMessage + '\'' +
               ", sourceType=" + sourceType +
               ", targetType=" + targetType +
               ", query='" + query + '\'' +
               ", parameters=" + parameters +
               '}';
    }

    // POC quality right there
    private Map<String, Object> deepCopy(Map<String, Object> parameters) {
        try {
            return new ObjectMapper().readValue(new ObjectMapper().writeValueAsBytes(parameters), new TypeReference<>() {
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
