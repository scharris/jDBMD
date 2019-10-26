package gov.fda.nctr.dbmd;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.sql.Types;
import java.util.Optional;
import static java.util.Objects.requireNonNull;


@JsonPropertyOrder({
  "name", "databaseType", "nullable", "primaryKeyPartNumber", "comment"
})
public class Field {

    private String name;

    private int jdbcTypeCode;

    private String databaseType;

    private Optional<Integer> length;

    private Optional<Integer> precision;

    private Optional<Integer> fractionalDigits;

    private Optional<Integer> radix;

    private Optional<Boolean> nullable;

    private Optional<Integer> primaryKeyPartNumber;

    private Optional<String> comment;

    public Field
        (
            String name,
            int jdbcTypeCode,
            String databaseType,
            Optional<Integer> length,
            Optional<Integer> precision,
            Optional<Integer> fractionalDigits,
            Optional<Integer> radix,
            Optional<Boolean> nullable,
            Optional<Integer> primaryKeyPartNumber,
            Optional<String> comment
        )
    {
        this.name = requireNonNull(name);
        this.jdbcTypeCode = requireNonNull(jdbcTypeCode);
        this.databaseType = requireNonNull(databaseType);
        this.length = requireNonNull(length);
        this.fractionalDigits = requireNonNull(fractionalDigits);
        this.radix = requireNonNull(radix);
        this.precision = requireNonNull(precision);
        this.nullable = requireNonNull(nullable);
        this.primaryKeyPartNumber = requireNonNull(primaryKeyPartNumber);
        this.comment = requireNonNull(comment);
    }

    protected Field() {}

    public String getName() { return name; }

    public int getJdbcTypeCode() { return jdbcTypeCode; }

    public String getDatabaseType() { return databaseType; }

    public Optional<Integer> getLength() { return length; }

    public Optional<Integer> getFractionalDigits() { return fractionalDigits; }

    public Optional<Integer> getRadix() { return radix; }

    public Optional<Integer> getPrecision() { return precision; }

    public Optional<Boolean> getNullable() { return nullable; }

    public Optional<Integer> getPrimaryKeyPartNumber() { return primaryKeyPartNumber; }

    public Optional<String> getComment() { return comment; }

    @JsonIgnore
    public boolean isNumericType() { return isJdbcTypeNumeric(jdbcTypeCode); }

    @JsonIgnore
    public boolean isCharacterType() { return isJdbcTypeChar(jdbcTypeCode); }

    public static boolean isJdbcTypeNumeric(int jdbcType)
    {
        switch (jdbcType)
        {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
        case Types.DECIMAL:
        case Types.NUMERIC:
            return true;
        default:
            return false;
        }
    }

    public static boolean isJdbcTypeChar(int jdbcType)
    {
        switch (jdbcType)
        {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return true;
        default:
            return false;
        }
    }
}