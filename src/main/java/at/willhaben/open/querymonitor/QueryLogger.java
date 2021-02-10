/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package at.willhaben.open.querymonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Log all calls to a database - with warnings for calls that take longer than a configurable limit.
 * <p>
 * An instance of this class is expected to be registered with a QueryMonitor (which wraps a JDBC DataSource).
 * </p>
 */
public class QueryLogger implements QueryReporterRegistry.Reporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryLogger.class);
    private static final String NEWLINE = System.getProperty("line.separator");

    private final long slowQueryLimitMillis;
    private final QueryNameExtractor queryNameExtractor;

    public QueryLogger(QueryReporterRegistry registry, long slowQueryLimitMillis) {
        this.slowQueryLimitMillis = slowQueryLimitMillis;
        this.queryNameExtractor = new QueryNameExtractor();
        registry.register(this);
    }

    @Override
    public void report(
            Class<?> statementType, String methodName,
            String sql, List<?> params,
            long durationMillis, Throwable exception) {
        String queryName = queryNameExtractor.extractQueryName(statementType, sql);

        if (exception != null) {
            LOGGER.warn("Exception occurred in JDBC: {}{}SQL being executed: {}",
                    exception.toString(),
                    NEWLINE,
                    sql);
        }

        if (durationMillis > slowQueryLimitMillis) {
            LOGGER.warn("Slow DB Query: call: {}, sql: {}, duration: {}ms", queryName, sql, durationMillis);
        }

        LOGGER.debug("query: {}, sql: {}", queryName, sql);
    }
}
