[log4jdbc]: <https://github.com/arthurblake/log4jdbc>

# jdbc-query-monitor

A Java JDBC wrapper for monitoring calls to a database (JDBC DataSource).

Class QueryMonitor implement the standard `javax.sql.DataSource` interface, and delegates to an underlying DataSource object - ie acts as a Proxy. When Statement/PreparedStatement/CallableStatement objects created via this proxy are actually "executed", info about the call is passed to registered "reporter" objects (aka listeners).

This project is similar in functionality to [log4jdbc] aka jdbc-spy. However:

*  the code here is simpler than log4jdbc
*  configuration does not rely on race-prone and functionally-limited static fields

An application using this code is responsible for wrapping datasources to be monitored; this is easiest when the application is using dependency-injection of some kind.

This code is provided "as-is". Although there is a maven build-file (pom) which supports building the code as a library, there is currently no intention to provide long-term support or maintenance for this code, or to publish this library to any maven repository. If you find this code useful, it is recommended that you just copy the source-code files into your project.
