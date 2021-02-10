/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package at.willhaben.open.querymonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Given some SQL that might represent a call to a stored-procedure, return the stored-procedure-name if possible.
 * <p>
 * This class is a helper which can optionally be used by classes which implement QueryReporterRegistry if they
 * wish to include "the stored procedure name" for calls made via a CallableStatement object.
 * </p>
 */
public class QueryNameExtractor {
    private static final Logger logger = LoggerFactory.getLogger(QueryNameExtractor.class);

    private static final Set<String> UNEXPECTED_PROC_NAMES = Set.of("call", "native", "unknown");
    private static final String DEFAULT_PATTERN = makeDefaultPattern();

    private final Pattern nameExtractorPattern;
    private final Set<String> badProcNames = new HashSet<>();

    /**
     * Define a regex that matches all the different possible ways to invoke a
     * stored procedure, and extract the procedure-name.
     */
    private static String makeDefaultPattern() {
        // Note that (somewhat unusual) expression "(?:..)" is a non-capture-group, ie it
        // groups a set of match-expressions, but does not return the matched text.

        StringBuilder sb = new StringBuilder();
        sb.append("\\s*"); // optional whitespace
        sb.append("(?:\\{\\s*)?"); // optional brace with optional trailing whitespace
        sb.append("(?:\\?\\s*\\=\\s*)?"); // optional "? =" wth optional trailing whitespace
        sb.append("(?:call\\s+|exec\\s+)?"); // optional call command with mandatory trailing whitespace
        sb.append("(\\w+)"); // the actual name to capture
        sb.append(".*"); // and the rest is irrelevant
        return sb.toString();
    }

    public QueryNameExtractor() {
        this(Pattern.compile(DEFAULT_PATTERN));
    }

    public QueryNameExtractor(Pattern pattern) {
        nameExtractorPattern = pattern;
    }

    private boolean isNativeQuery(String sql) {
        sql = sql.trim().toLowerCase();
        return sql.startsWith("select ")
                || sql.startsWith("insert ")
                || sql.startsWith("update ")
                || sql.startsWith("delete ");
    }

    /**
     * Try to identify whether this is a call to a stored-procedure; if so return the stored-procedure-name.
     * <p>
     * When this is not a call to a stored procedure, just return string "native". When it is not possible
     * to figure out what this is, return string "unknown".
     * </p>
     */
    public String extractQueryName(Class<?> statementType, String sql) {
        // Unfortunately, stored procedures can be invoked via normal Statement or PreparedStatement calls,
        // via SQL syntax like "storedProcedureName....". So simply checking for statementType=CallableStatement
        // is not sufficient.
        if ((CallableStatement.class != statementType) && isNativeQuery(sql)) {
            return "native";
        }

        // Try to extract the procedure name. The sql can be any of:
        // * storedProcedureName
        // * storedProcedureName p1,p2,...
        // * call storedProcedureName ...
        // * {call storedProcedureName}
        // * {call storedProcedureName(p1,p2, ..)}
        // * {? = call storedProcedureName(?)}
        Matcher matcher = nameExtractorPattern.matcher(sql);
        if (matcher.matches()) {
            String procName = matcher.group(1);
            if (procName == null || procName.isEmpty()) {
                logBadProcName(sql);
                procName = "unmatched";
            } else if (UNEXPECTED_PROC_NAMES.contains(procName)) {
                logBadProcName(sql);
            }
            return procName;
        }

        // It isn't clear what this is..
        logBadProcName(sql);
        return "unknown";
    }

    /**
     * A stored-proc name could not be extracted from the provided SQL; log it so developers can investigate and
     * improve the nameExtractorPattern.
     * <p>
     * Log the specified SQL just once (per node) to avoid log-spam.
     * </p>
     */
    private synchronized void logBadProcName(String sql) {
        if (badProcNames.size() > 1000) {
            // Too many different "bad" SQL statements have been found, ie too much memory is being
            // used to do the "log once only" feature. Just stop logging at this point..
            return;
        }

        if (!badProcNames.contains(sql)) {
            badProcNames.add(sql); // suppress future logging of this same SQL
            logger.warn(
                    "Cannot extract stored procedure name from sql '{}' using pattern '{}'",
                    sql, nameExtractorPattern.pattern());
        }
    }
}
