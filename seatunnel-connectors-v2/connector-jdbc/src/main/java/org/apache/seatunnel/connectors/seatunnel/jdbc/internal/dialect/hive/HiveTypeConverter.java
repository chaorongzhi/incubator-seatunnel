/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.hive;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

// reference https://eco.dameng.com/document/dm/zh-cn/sql-dev/dmpl-sql-datatype.html
@Slf4j
@AutoService(TypeConverter.class)
public class HiveTypeConverter implements TypeConverter<BasicTypeDefine> {
    // ============================data types=====================
    private static final Logger LOG = LoggerFactory.getLogger(HiveTypeMapper.class);

    // reference https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types

    // Numeric Types
    private static final String HIVE_TINYINT = "TINYINT";
    private static final String HIVE_SMALLINT = "SMALLINT";
    private static final String HIVE_INT = "INT";
    private static final String HIVE_INTEGER = "INTEGER";
    private static final String HIVE_BIGINT = "BIGINT";
    private static final String HIVE_FLOAT = "FLOAT";
    private static final String HIVE_DOUBLE = "DOUBLE";
    private static final String HIVE_DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String HIVE_DECIMAL = "DECIMAL";
    private static final String HIVE_NUMERIC = "NUMERIC";
    // Date/Time Types
    private static final String HIVE_TIMESTAMP = "TIMESTAMP";
    private static final String HIVE_DATE = "DATE";
    private static final String HIVE_INTERVAL = "INTERVAL";
    // String Types
    private static final String HIVE_STRING = "STRING";
    private static final String HIVE_VARCHAR = "VARCHAR";
    private static final String HIVE_CHAR = "CHAR";
    // Misc Types
    private static final String HIVE_BOOLEAN = "BOOLEAN";
    private static final String HIVE_BINARY = "BINARY";
    // Complex Types
    private static final String HIVE_ARRAY = "ARRAY";
    private static final String HIVE_MAP = "MAP";
    private static final String HIVE_STRUCT = "STRUCT";
    private static final String HIVE_UNIONTYPE = "UNIONTYPE";
    public static final HiveTypeConverter INSTANCE = new HiveTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.DAMENG;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String dmType = typeDefine.getDataType().toUpperCase();
        switch (dmType) {
            case HIVE_TINYINT:
                builder.sourceType(HIVE_TINYINT);
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case HIVE_SMALLINT:
                builder.sourceType(HIVE_SMALLINT);
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case HIVE_INT:
                builder.sourceType(HIVE_INT);
                builder.dataType(BasicType.INT_TYPE);
                break;
            case HIVE_INTEGER:
                builder.sourceType(HIVE_INTEGER);
                builder.dataType(BasicType.INT_TYPE);
                break;
            case HIVE_BIGINT:
                builder.sourceType(HIVE_BIGINT);
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case HIVE_FLOAT:
                builder.sourceType(HIVE_FLOAT);
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case HIVE_DOUBLE:
                builder.sourceType(HIVE_DOUBLE);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case HIVE_DOUBLE_PRECISION:
                builder.sourceType(HIVE_DOUBLE_PRECISION);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case HIVE_DECIMAL:
            case HIVE_NUMERIC:
                DecimalType decimalType;
                if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
                    decimalType =
                            new DecimalType(
                                    typeDefine.getPrecision().intValue(), typeDefine.getScale());
                } else {
                    LOG.warn("decimal did define precision,scale, will be Decimal(38,18)");
                    decimalType = new DecimalType(38, 18);
                }
                builder.sourceType(
                        String.format(
                                "%s(%s,%s)",
                                HIVE_NUMERIC, decimalType.getPrecision(), decimalType.getScale()));
                builder.dataType(decimalType);
                builder.columnLength((long) decimalType.getPrecision());
                builder.scale(decimalType.getScale());
                break;
            case HIVE_TIMESTAMP:
                builder.sourceType(HIVE_TIMESTAMP);
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            case HIVE_DATE:
                builder.sourceType(HIVE_DATE);
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case HIVE_STRING:
                builder.sourceType(HIVE_STRING);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case HIVE_VARCHAR:
                builder.sourceType(HIVE_VARCHAR);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case HIVE_CHAR:
                builder.sourceType(HIVE_CHAR);
                builder.dataType(BasicType.STRING_TYPE);
                builder.columnLength(typeDefine.getLength());
                break;
            case HIVE_BOOLEAN:
                builder.sourceType(HIVE_BOOLEAN);
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case HIVE_BINARY:
            case HIVE_ARRAY:
            case HIVE_INTERVAL:
            case HIVE_MAP:
            case HIVE_STRUCT:
            case HIVE_UNIONTYPE:
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.HIVE, typeDefine.getDataType(), typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        return null;
    }
}
