/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package at.willhaben.open.querymonitor;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class QueryMonitorTest {
    private static class MockReporter implements QueryReporterRegistry.Reporter {
        List<String> reported = new ArrayList<>();
        @Override
        public void report(Class<?> statementType, String methodName, String sql, List<?> params, long durationMillis, Throwable exception) {
            String paramStr = params.stream().map(String::valueOf).collect(Collectors.joining(","));
            reported.add(String.format("sql:'%s' interval:%d hasException:%s nparams:%d params:%s",
                    sql, durationMillis, (exception != null), params.size(), paramStr));
        }
    }

    @Test
    public void testBasics() throws Exception {
        Statement statement = Mockito.mock(Statement.class);
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
        DataSource dataSource = Mockito.mock(DataSource.class);
        Mockito.when(dataSource.getConnection()).thenReturn(connection);

        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000), ZoneId.systemDefault());
        QueryMonitor queryMonitor = new QueryMonitor(clock, dataSource);

        MockReporter mockReporter = new MockReporter();
        queryMonitor.register(mockReporter);

        // Plain Statement
        queryMonitor.getConnection().createStatement().execute("hello world");
        Assert.assertEquals(1, mockReporter.reported.size());
        Assert.assertEquals("sql:'hello world' interval:0 hasException:false nparams:0 params:", mockReporter.reported.get(0));

        // PreparedStatement with no params
        mockReporter.reported.clear();
        queryMonitor.getConnection().prepareStatement("prepare world").execute();
        Assert.assertEquals(1, mockReporter.reported.size());
        Assert.assertEquals("sql:'prepare world' interval:0 hasException:false nparams:0 params:", mockReporter.reported.get(0));

        // PreparedStatement with 1 param
        mockReporter.reported.clear();
        {
            PreparedStatement ps = queryMonitor.getConnection().prepareStatement("prepare world");
            ps.setString(1, "firstParam");
            ps.execute();
        }
        Assert.assertEquals(1, mockReporter.reported.size());
        Assert.assertEquals("sql:'prepare world' interval:0 hasException:false nparams:1 params:firstParam", mockReporter.reported.get(0));

        // PreparedStatement with multiple params
        mockReporter.reported.clear();
        {
            PreparedStatement ps = queryMonitor.getConnection().prepareStatement("prepare world");
            ps.setBoolean(1, true);
            ps.setDouble(2, 12.5);
            // item 3 deliberately missing
            ps.setTimestamp(4, new Timestamp(4000));
            ps.setBigDecimal(6, BigDecimal.TEN);
            ps.setNull(5, Types.CHAR); // deliberately out-of-order
            ps.execute();
        }
        Assert.assertEquals(1, mockReporter.reported.size());
        Assert.assertEquals(
                "sql:'prepare world' interval:0 hasException:false nparams:6"
                + " params:true,12.5,null,1970-01-01 01:00:04.0,null,10",
                mockReporter.reported.get(0));
    }
}
