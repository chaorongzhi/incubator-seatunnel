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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.clickhouse;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ClickhouseTypeMapper implements JdbcDialectTypeMapper {

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String clickhouseType = metadata.getColumnTypeName(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (clickhouseType) {
            case "UInt32":
            case "UInt64":
            case "Int64":
            case "IntervalYear":
            case "IntervalQuarter":
            case "IntervalMonth":
            case "IntervalWeek":
            case "IntervalDay":
            case "IntervalHour":
            case "IntervalMinute":
            case "IntervalSecond":
                return BasicType.LONG_TYPE;
            case "Float64":
                return BasicType.DOUBLE_TYPE;
            case "Float32":
                return BasicType.FLOAT_TYPE;
            case "Int8":
            case "UInt8":
            case "Int16":
            case "UInt16":
            case "Int32":
                return BasicType.INT_TYPE;
            case "Date":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "DateTime":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "Decimal":
            case "Decimal64":
            case "Decimal128":
            case "Decimal256":
                return new DecimalType(precision, scale);
            case "String":
            case "Int128":
            case "UInt128":
            case "Int256":
            case "UInt256":
            case "Point":
            case "Polygon":
            default:
                return BasicType.STRING_TYPE;
        }
    }
}
