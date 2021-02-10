/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package at.willhaben.open.querymonitor;

import org.junit.Assert;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.Map;

public class QueryNameExtractorTest {
    @Test
    public void testBasics() {
        QueryNameExtractor qne = new QueryNameExtractor(null);
        Assert.assertEquals("native", qne.extractQueryName(PreparedStatement.class, "select * from table"));
        Assert.assertEquals("pr_abc1", qne.extractQueryName(CallableStatement.class, "pr_abc1"));
        Assert.assertEquals("pr_abc2", qne.extractQueryName(CallableStatement.class, "pr_abc2 ?,?,?"));
        Assert.assertEquals("pr_abc3", qne.extractQueryName(CallableStatement.class, "{call pr_abc3}"));
        Assert.assertEquals("pr_abc4", qne.extractQueryName(CallableStatement.class, "{call pr_abc4()}"));
        Assert.assertEquals("pr_abc5", qne.extractQueryName(CallableStatement.class, "{call pr_abc5(?,?)}"));
        Assert.assertEquals("pr_abc5", qne.extractQueryName(CallableStatement.class, " {  call pr_abc5(?,?)} "));
        Assert.assertEquals("pr_abc6", qne.extractQueryName(CallableStatement.class, "call pr_abc6"));
        Assert.assertEquals("pr_abc6", qne.extractQueryName(CallableStatement.class, "  call   pr_abc6  "));

        Assert.assertEquals("unknown", qne.extractQueryName(CallableStatement.class, ""));
        Assert.assertEquals("unknown", qne.extractQueryName(CallableStatement.class, "{}"));
        Assert.assertEquals("select", qne.extractQueryName(CallableStatement.class, "select a from foo"));
        Assert.assertEquals("select", qne.extractQueryName(CallableStatement.class, "  select a from foo"));
        Assert.assertEquals("native", qne.extractQueryName(PreparedStatement.class, "select a, b from foo"));
        Assert.assertEquals("native", qne.extractQueryName(PreparedStatement.class, "  select a, b from foo"));
    }

    @Test
    public void testMoreCases() {
        Map<String, String> tests = new LinkedHashMap<>();
        tests.put("{call pr_abc(794872421)}", "pr_abc");
        tests.put("{call pr_abc('XYZ','|')}", "pr_abc");
        tests.put("{call pr_abc()}", "pr_abc");
        tests.put("{call pr_abc ('arg1','arg2',0,1000,-1,100)}", "pr_abc");
        tests.put("{? = call pr_abc(?)}", "pr_abc");
        tests.put("{ call pr_abc(?, ?, ?, ?) }", "pr_abc");
        tests.put("exec pr_abc :someRef", "pr_abc");
        tests.put("pr_abc null,'arg1',null,null,null,null,null,null,null,null,null,null,null,null,0", "pr_abc");
        tests.put("pr_abc 12345", "pr_abc");
        tests.put("call pr_abc", "pr_abc");
        tests.put("pr_abc", "pr_abc");
        tests.put("exec pr_abc", "pr_abc");
        tests.put("execabc", "execabc"); // SP name happens to begin with "exec.."

        QueryNameExtractor qne = new QueryNameExtractor(null);
        tests.forEach((key, value) ->
                Assert.assertEquals(value, qne.extractQueryName(CallableStatement.class, key)));
    }
}
