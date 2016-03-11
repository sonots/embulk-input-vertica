package org.embulk.input.vertica;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.Task;
import org.embulk.spi.time.TimestampFormat;
import org.embulk.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;

import com.google.common.base.Optional;

public interface VerticaTypeOption
        extends Task
{
    @Config("value_type")
    @ConfigDefault("\"coalesce\"")
    public String getValueType();

    @Config("type")
    @ConfigDefault("null")
    public Optional<Type> getType();

    @Config("timestamp_format")
    @ConfigDefault("null")
    public Optional<TimestampFormat> getTimestampFormat();

    @Config("timezone")
    @ConfigDefault("null")
    public Optional<DateTimeZone> getTimeZone();

    // required by TimestampFormatter
    @ConfigInject
    public ScriptingContainer getJRuby();
}
