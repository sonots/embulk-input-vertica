package org.embulk.input.vertica.getter;

import java.util.Map;

import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.vertica.VerticaTypeOption;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTimeZone;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;  
import org.joda.time.DateTimeZone;

import org.embulk.config.ConfigException;
import org.embulk.input.jdbc.getter.LongColumnGetter;
import org.embulk.input.jdbc.getter.FloatColumnGetter;
import org.embulk.input.jdbc.getter.DoubleColumnGetter;
import org.embulk.input.jdbc.getter.BooleanColumnGetter;
import org.embulk.input.jdbc.getter.StringColumnGetter;
//import org.embulk.input.jdbc.getter.JsonColumnGetter;
import org.embulk.input.jdbc.getter.DateColumnGetter;
import org.embulk.input.jdbc.getter.TimeColumnGetter;
import org.embulk.input.jdbc.getter.TimestampColumnGetter;
import org.embulk.input.jdbc.getter.BigDecimalColumnGetter;

public class VerticaColumnGetterFactory extends ColumnGetterFactory
{
    private Map<String, VerticaTypeOption> typeOptions;
    public static final String DATE_COLUMN_DEFAULT_FORMAT = "%Y-%m-%d";
    private final PageBuilder to;
    private final DateTimeZone defaultTimeZone;

    public VerticaColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone,
            Map<String, VerticaTypeOption> typeOptions)
    {
        super(to, defaultTimeZone);
        this.to = to;
        this.defaultTimeZone = defaultTimeZone;
        this.typeOptions = typeOptions;
    }

    private ColumnGetter newColumnGetter(JdbcColumn column, JdbcColumnOption option, String valueType)
    {
        VerticaTypeOption typeOption = typeOptions.get(valueType);
        Type toType = getToType(option, typeOption);
        switch(valueType) {
        // case "coalesce":
        //     return newColumnGetter(column, option, sqlTypeToValueType(column, column.getSqlType()));
        case "long":
            return new LongColumnGetter(to, toType);
        case "float":
            return new FloatColumnGetter(to, toType);
        case "double":
            return new DoubleColumnGetter(to, toType);
        case "boolean":
            return new BooleanColumnGetter(to, toType);
        case "string":
            return new StringColumnGetter(to, toType);
        // case "json":
        //     return new JsonColumnGetter(to, toType);
        case "date":
            return new DateColumnGetter(to, toType, newTimestampFormatter(option, typeOption, DATE_COLUMN_DEFAULT_FORMAT));
        // case "time":
        //     return new TimeColumnGetter(to, toType, newTimestampFormatter(option, typeOption, DateColumnGetter.DEFAULT_FORMAT));
        // case "timestamp":
        //     return new TimestampColumnGetter(to, toType, newTimestampFormatter(option, typeOption, DateColumnGetter.DEFAULT_FORMAT));
        // case "decimal":
        //     return new BigDecimalColumnGetter(to, toType);
        // default:
        //     throw new ConfigException(String.format("Unknown value_type '%s' for column '%s'", option.getValueType(), column.getName()));
        }
        return super.newColumnGetter(column, option, valueType);
    }

    private Type getToType(JdbcColumnOption option, VerticaTypeOption typeOption)
    {
        Type toType = null;
        if (typeOption != null) {
            if (typeOption.getType().isPresent()) {
                toType = typeOption.getType().get();
            }
        }
        if (toType == null) {
            toType = getToType(option);
        }
        return toType;
    }

    private String getTimestampFormat(JdbcColumnOption option, VerticaTypeOption typeOption,
            String defaultTimestampFormat)
    {
        String timestampFormat = null;
        if (typeOption != null) {
            if (typeOption.getTimestampFormat().isPresent()) {
                timestampFormat = typeOption.getTimestampFormat().get().getFormat();
            }
        }
        if (timestampFormat == null) {
            timestampFormat = option.getTimestampFormat().isPresent() ?
                option.getTimestampFormat().get().getFormat() : defaultTimestampFormat;
        }
        return timestampFormat;
    }

    private DateTimeZone getTimezone(JdbcColumnOption option, VerticaTypeOption typeOption)
    {
        DateTimeZone timezone = null;
        if (typeOption != null) {
            if (typeOption.getTimeZone().isPresent()) {
                timezone = typeOption.getTimeZone();
            }
        }
        if (timezone == null) {
            timezone = option.getTimeZone().or(defaultTimeZone);
        }
        return timezone;
    }

    private TimestampFormatter newTimestampFormatter(JdbcColumnOption option,
            VerticaTypeOption typeOption, String defaultTimestampFormat)
    {
        return new TimestampFormatter(
                option.getJRuby(),
                getTimestampFormat(option, typeOption, defaultTimestampFormat),
                getTimezone(option, typeOption));
    }
}
