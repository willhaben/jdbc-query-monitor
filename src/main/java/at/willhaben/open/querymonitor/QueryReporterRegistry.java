/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package at.willhaben.open.querymonitor;

import java.util.List;

/**
 * Maintains a list of registered Reporter instances which should be notified of calls to a database.
 *
 * @See {QueryMonitor}.
 */
public interface QueryReporterRegistry {
    interface Reporter {
        /**
         * Callback method providing information about a database operation that has just completed.
         *
         * @param statementType is one of (Statement.class, PreparedStatement.class, CallableStatement.class, Void.class)
         * @param methodName is the JDBC method invoked, eg "executeQuery".
         * @param sql is the SQL that was passed to the DB for execution
         * @param params are the parameters set on a PreparedStatement/CallableStatement (empty list for plain statements)
         * @param durationMillis is the duration of the call in milliseconds
         * @param exception is non-null when the operation threw an exception
         */
        void report(Class<?> statementType, String methodName, String sql, List<?> params, long durationMillis, Throwable exception);
    }

    void register(Reporter reporter);
}
