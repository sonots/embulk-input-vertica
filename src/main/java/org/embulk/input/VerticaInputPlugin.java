package org.embulk.input;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.JdbcSchema;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.vertica.VerticaInputConnection;
import org.embulk.input.vertica.VerticaTypeOption;
import org.embulk.input.vertica.getter.VerticaColumnGetterFactory;
import org.embulk.spi.Exec;
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

        @Config("type_options")
        @ConfigDefault("{}")
        public Map<String, VerticaTypeOption> getTypeOptions();}

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

    @Override
    private List<ColumnGetter> newColumnGetters(PluginTask task, JdbcSchema querySchema, PageBuilder pageBuilder)
            throws SQLException
    {
        ColumnGetterFactory factory = newColumnGetterFactory(task, pageBuilder, task.getDefaultTimeZone());
        ImmutableList.Builder<ColumnGetter> getters = ImmutableList.builder();
        for (JdbcColumn c : querySchema.getColumns()) {
            JdbcColumnOption columnOption = columnOptionOf(task.getColumnOptions(), c);
            getters.add(factory.newColumnGetter(c, columnOption));
        }
        return getters.build();
    }

    private static JdbcColumnOption columnOptionOf(Map<String, JdbcColumnOption> columnOptions, JdbcColumn targetColumn)
    {
        return Optional.fromNullable(columnOptions.get(targetColumn.getName())).or(
                    // default column option
                    new Supplier<JdbcColumnOption>()
                    {
                        public JdbcColumnOption get()
                        {
                            return Exec.newConfigSource().loadConfig(JdbcColumnOption.class);
                        }
                    });
    }

    protected ColumnGetterFactory newColumnGetterFactory(VerticaPluginTask task, PageBuilder pageBuilder, DateTimeZone dateTimeZone)
    {
        return new VerticaColumnGetterFactory(pageBuilder, dateTimeZone, task.getTypeOptions());
    }
}
