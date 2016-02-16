package org.embulk.input;

import java.util.Properties;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.vertica.VerticaInputConnection;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTimeZone;

public class VerticaInputPlugin
        extends AbstractJdbcInputPlugin
{
    private static final Driver driver = new com.vertica.jdbc.Driver();

    public interface VerticaPluginTask
            extends PluginTask
    {
        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("5433")
        public int getPort();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

        @Config("database")
        public String getDatabase();

        @Config("schema")
        @ConfigDefault("\"public\"")
        public String getSchema();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return VerticaPluginTask.class;
    }

    @Override
    protected VerticaInputConnection newConnection(PluginTask task) throws SQLException
    {
        VerticaPluginTask t = (VerticaPluginTask) task;

        String url = String.format("jdbc:vertica://%s:%d/%s",
                t.getHost(), t.getPort(), t.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", t.getUser());
        props.setProperty("password", t.getPassword());
        props.setProperty("loginTimeout", String.valueOf(t.getConnectTimeout())); // seconds
        props.setProperty("socketTimeout", String.valueOf(t.getSocketTimeout())); // seconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        // if (t.getSsl()) {
        //     // TODO add ssl_verify (boolean) option to allow users to verify certification.
        //     //      see embulk-input-ftp for SSL implementation.
        //     props.setProperty("ssl", "true");
        //     props.setProperty("sslfactory", "org.vertica.ssl.NonValidatingFactory");  // disable server-side validation
        // }
        // // setting ssl=false enables SSL. See org.vertica.core.v3.openConnectionImpl.

        props.putAll(t.getOptions());

        Connection con = driver.connect(url, props);
        try {
            VerticaInputConnection c = new VerticaInputConnection(con, t.getSchema());
            con = null;
            return c;
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }
}
