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

    private String dbTypeName;

    private Optional<Integer> length;

    private Optional<Integer> precision;

    private Optional<Integer> fractionalDigits;

    private Optional<Integer> radix;

    private Optional<Boolean> isNullable;

    private Optional<Integer> pkPartNum;

    private Optional<String> comment;

    public Field
        (
            String name,
            int jdbcTypeCode,
            String dbTypeName,
            Optional<Integer> length,
            Optional<Integer> precision,
            Optional<Integer> fractionalDigits,
            Optional<Integer> radix,
            Optional<Boolean> isNullable,
            Optional<Integer> pkPartNum,
            Optional<String> comment
        )
    {
        this.name = requireNonNull(name);
        this.jdbcTypeCode = requireNonNull(jdbcTypeCode);
        this.dbTypeName = requireNonNull(dbTypeName);
        this.length = requireNonNull(length);
        this.fractionalDigits = requireNonNull(fractionalDigits);
        this.radix = requireNonNull(radix);
        this.precision = requireNonNull(precision);
        this.isNullable = requireNonNull(isNullable);
        this.pkPartNum = requireNonNull(pkPartNum);
        this.comment = requireNonNull(comment);
    }

    protected Field() {}

    public String getName() { return name; }

    public int getJdbcTypeCode() { return jdbcTypeCode; }

    public String getDatabaseType() { return dbTypeName; }

    public Optional<Integer> getLength() { return length; }

    public Optional<Integer> getFractionalDigits() { return fractionalDigits; }

    public Optional<Integer> getRadix() { return radix; }

    public Optional<Integer> getPrecision() { return precision; }

    public Optional<Boolean> getNullable() { return isNullable; }

    public Optional<Integer> getPrimaryKeyPartNumber() { return pkPartNum; }

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