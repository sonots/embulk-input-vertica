package org.embulk.input.vertica;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.embulk.spi.Exec;
import org.embulk.input.jdbc.JdbcInputConnection;

public class VerticaInputConnection
        extends JdbcInputConnection
{
    private final Logger logger = Exec.getLogger(VerticaInputConnection.class);

    public VerticaInputConnection(Connection connection, String schemaName)
            throws SQLException
    {
        super(connection, schemaName);
    }
}
