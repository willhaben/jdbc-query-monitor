/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package at.willhaben.open.querymonitor;

/**
 * Registry which does nothing with the registered objects; useful in tests and in environments
 * where DB monitoring is not important.
 */
public class QueryReporterNoOpRegistry implements QueryReporterRegistry {
    @Override
    public void register(Reporter reporter) {
    }
}
