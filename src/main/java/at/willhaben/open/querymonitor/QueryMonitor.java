/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package at.willhaben.open.querymonitor;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.time.Clock;
import java.util.*;
import java.util.logging.Logger;

/**
 * Monitors calls to a database, allowing logging/metrics to be recorded.
 * <p>
 * This class maintains a list of registered ReporterRegistry.Reporter instances. It also wraps a DataSource instance
 * and ensures that all DB operations executed via Statement, PreparedStatement and CallableStatement objects created
 * from that DataSource trigger callbacks to the reporter instances.
 * </p>
 * <p>
 * It is expected that any DataSource instance used by the application is wrapped in an instance of this type,
 * and this wrapper is then injected instead of the original DataSource.
 * </p>
 * <p>
 * The number of methods on standard interface DataSource is relatively low, so this class simply implements
 * DataSource directly, and forwards calls to the wrapped instance as appropriate. However interfaces
 * Connection, Statement, PreparedStatement and CallableStatement are far more complex; Java dynamic proxy
 * generation is therefore used to wrap these objects.
 * </p>
 */
public class QueryMonitor implements DataSource, QueryReporterRegistry {
    private static final ClassLoader connectionClassLoader = Connection.class.getClassLoader();
    private static final Class[] connectionInterfaces = new Class[] { Connection.class };

    private static final ClassLoader statementClassLoader = Statement.class.getClassLoader();
    private static final Class[] statementInterfaces = new Class[] { Statement.class };

    private static final ClassLoader preparedStatementClassLoader = PreparedStatement.class.getClassLoader();
    private static final Class[] preparedStatementInterfaces = new Class[] { PreparedStatement.class };

    private static final ClassLoader callableStatementClassLoader = CallableStatement.class.getClassLoader();
    private static final Class[] callableStatementInterfaces = new Class[] { CallableStatement.class };

    // Warning: the order of the elements in this list is important..
    private static final List<Class<?>> statementTypes = List.of(
            CallableStatement.class, PreparedStatement.class, Statement.class);

    // Names of all "param setter methods" on interface PreparedStatement; calls to these methods cause
    // the (index, param) pair to be added to the params list which is then passed to the reporter
    private static final Set<String> PARAM_SETTERS = Set.of(
            "setBigDecimal",
            "setBoolean",
            "setByte",
            "setDate",
            "setDouble",
            "setFloat",
            "setInt",
            "setLong",
            "setNull",
            "setObject",
            "setShort",
            "setString",
            "setTime",
            "setTimestamp");

    /** Source of time data */
    private final Clock clock;

    /** The datasource to which calls should be intercepted/reported. */
    private final DataSource wrapped;

    /** The reporters which should be notified of DB calls. */
    private final List<Reporter> reporters = new ArrayList<>();

    /** Constructor. */
    public QueryMonitor(Clock clock, DataSource wrapped) {
        this.clock = clock;
        this.wrapped = wrapped;
    }

    /** Constructor. */
    public QueryMonitor(DataSource wrapped) {
        this(Clock.systemUTC(), wrapped);
    }

    /**
     * Register an object which will be notified of every DML operation sent to the database.
     */
    @Override
    public void register(Reporter reporter) {
        reporters.add(reporter);
    }

    /**
     * Invoked by statement-proxies to execute the actual DB operation and invoke a callback on the
     * registered reporters.
     *
     * @param target is the wrapped JDBC statement object that actually performs the DB operation
     * @param method is the method to be invoked on the statement object (eg executeQuery).
     * @param args are the parameters to the method (not the args to the SQL!)
     * @param sql is the string that will be sent to the DB
     * @param params are the parameters set on a PreparedStatement (if any)
     * @return whatever the specified method returns
     * @throws Throwable whatever the specified method throws
     */
    protected Object invokeAndReport(Object target, Method method, Object[] args, String sql, List<?> params) throws Throwable {
        // execute and monitor
        long timeStart = clock.millis();
        Throwable exception = null;
        Object result = null;
        try {
            result = method.invoke(target, args);
        } catch(Throwable t) {
            exception = t;
        }
        long duration = clock.millis() - timeStart;

        // report
        Class<?> statementType = toStatementType(target);
        String methodName = method.getName();
        for(Reporter reporter: reporters) {
            reporter.report(statementType, methodName, sql, params, duration, exception);
        }

        // return to caller
        if (exception != null) {
            throw exception;
        }
        return result;
    }

    /**
     * Given some JDBC statement subtype, return the JDBC interface.
     */
    private static Class<?> toStatementType(Object target) {
        return statementTypes.stream().filter(type -> type.isInstance(target)).findFirst().orElse(Void.class);
    }

    /** When a new connection is generated, return a proxy that intercepts calls to it. */
    @Override
    public Connection getConnection() throws SQLException {
        return (Connection) Proxy.newProxyInstance(
                connectionClassLoader,
                connectionInterfaces,
                new ConnectionHandler(wrapped.getConnection()));
    }

    /** When a new connection is generated, return a proxy that intercepts calls to it. */
    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return (Connection) Proxy.newProxyInstance(
                connectionClassLoader,
                connectionInterfaces,
                new ConnectionHandler(wrapped.getConnection(username, password)));
    }

    /**
     * This method is rather difficult to implement with proxying. Fortunately this method is very seldom used -
     * and most/all users will call isWrapperFor first.
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("unwrap is not supported by QueryMonitor");
    }

    /**
     * This method is rather difficult to implement with proxying. Fortunately this method is very seldom used,
     * and is usually optional anyway.
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    // ========== standard DataSource methods

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return wrapped.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        wrapped.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        wrapped.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return wrapped.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return wrapped.getParentLogger();
    }

    // ==================================================================================================

    /** Dynamic proxy for Connection instances. */
    private class ConnectionHandler implements InvocationHandler {
        private final Connection wrapped;

        ConnectionHandler(Connection wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();

            if ("createStatement".equals(methodName)) {
                Statement statement = (Statement) method.invoke(wrapped, args);
                return Proxy.newProxyInstance(
                        statementClassLoader,
                        statementInterfaces,
                        new StatementHandler(statement, (Connection) proxy));
            }

            if ("prepareStatement".equals(methodName)) {
                String sql = (String) args[0];
                PreparedStatement statement = (PreparedStatement) method.invoke(wrapped, args);
                return Proxy.newProxyInstance(
                        preparedStatementClassLoader,
                        preparedStatementInterfaces,
                        new PreparedStatementHandler(statement, (Connection) proxy, sql));
            }

            if ("prepareCall".equals(methodName)) {
                String sql = (String) args[0];
                CallableStatement statement = (CallableStatement) method.invoke(wrapped, args);
                // CallableStatement is a subclass of PreparedStatement, and the proxy-handler logic
                // needed is actually identical, so PreparedStatementHandler can be reused here.
                return Proxy.newProxyInstance(
                        callableStatementClassLoader,
                        callableStatementInterfaces,
                        new PreparedStatementHandler(statement, (Connection) proxy, sql));
            }

            return method.invoke(wrapped, args);
        }

        public String toString() {
            return "QueryMonitor jdbc proxy for " + wrapped.toString();
        }
    }

    /** Dynamic proxy for Statement instances. */
    private class StatementHandler implements InvocationHandler {
        private final Statement wrapped;
        private final Connection connection;

        StatementHandler(Statement wrapped, Connection connection) {
            this.wrapped = wrapped;
            this.connection = connection;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();

            // Note: executeBatch not intercepted yet..
            if ("execute".equals(methodName) || "executeQuery".equals(methodName) || "executeUpdate".equals(methodName)) {
                return invokeAndReport(wrapped, method, args, (String) args[0], Collections.emptyList());
            }

            if ("getConnection".equals(methodName)) {
                // ensure proxying is retained
                return connection;
            }

            return method.invoke(wrapped, args);
        }

        public String toString() {
            return "QueryMonitor jdbc proxy for " + wrapped.toString();
        }
    }

    /** Dynamic proxy for PreparedStatement instances. */
    private class PreparedStatementHandler implements InvocationHandler {
        private static final int MAX_PARAMS = 30; // for memory use purposes, only record the first N params

        private final PreparedStatement wrapped;
        private final Connection connection;
        private final String sql;

        // SQL params set via setter-calls
        private Object[] params = new Object[MAX_PARAMS];
        private int paramCount = 0; // (index+1) of highest entry set in params array

        PreparedStatementHandler(PreparedStatement wrapped, Connection connection, String sql) {
            this.wrapped = wrapped;
            this.connection = connection;
            this.sql = sql;
        }

        private void storeParam(Object index, Object value) {
            if (!(index instanceof Integer)) {
                return; // unexpected..
            }

            int idx = (Integer) index;
            if ((idx <= 0) || (idx > params.length)) {
                return; // <= 0 is unexpected; >params.length is just unsupported
            }

            params[idx-1] = value;
            paramCount = Math.max(paramCount, idx);
        }

        private List<Object> paramsToList() {
            if (paramCount == 0) {
                return List.of();
            }
            return new ArrayBackedList(paramCount, params);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();

            // Note: executeBatch not intercepted yet..
            if ("execute".equals(methodName) || "executeQuery".equals(methodName) || "executeUpdate".equals(methodName)) {
                return invokeAndReport(wrapped, method, args, sql, paramsToList());
            }

            if ("getConnection".equals(methodName)) {
                // ensure proxying is retained
                return connection;
            }

            // remember the stored params
            if ("setNull".equals(methodName)) {
                storeParam(args[0], null);
            } else if (PARAM_SETTERS.contains(methodName) && (args.length >= 2)) {
                storeParam(args[0], args[1]);
            }

            return method.invoke(wrapped, args);
        }

        public String toString() {
            return "QueryMonitor jdbc proxy for " + wrapped.toString();
        }
    }

    private static class ArrayBackedList extends AbstractList<Object> {
        private final int size;
        private final Object[] data;

        ArrayBackedList(int size, Object[] data) {
            this.size = size;
            this.data = data;
        }

        @Override
        public Object get(int index) {
            if (index >= size) {
                throw new IndexOutOfBoundsException();
            }
            return data[index];
        }

        @Override
        public int size() {
            return size;
        }
    }
}
