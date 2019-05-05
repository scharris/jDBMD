package gov.fda.nctr.dbmd;

import java.sql.Types;


public class Field {

    private String name;

    private int jdbcTypeCode;

    private String dbTypeName;

    private Integer length;

    private Integer precision;

    private Integer fractionalDigits;

    private Integer radix;

    private Boolean isNullable;

    private Integer pkPartNum;

    private String comment;

    public Field
        (
            String name,
            int jdbcTypeCode,
            String dbTypeName,
            Integer length,
            Integer precision,
            Integer fractionalDigits,
            Integer radix,
            Boolean isNullable,
            Integer pkPartNum,
            String comment
        )
    {
        this.name = name;
        this.jdbcTypeCode = jdbcTypeCode;
        this.dbTypeName = dbTypeName;
        this.length = length;
        this.fractionalDigits = fractionalDigits;
        this.radix = radix;
        this.precision = precision;
        this.isNullable = isNullable;
        this.pkPartNum = pkPartNum;
        this.comment = comment;
    }

    protected Field() {}

    public String getName() { return name; }

    public int getJdbcTypeCode() { return jdbcTypeCode; }

    public String getDatabaseType() { return dbTypeName; }

    public Integer getLength() { return length; }

    public Integer getFractionalDigits() { return fractionalDigits; }

    public Integer getRadix() { return radix; }

    public Integer getPrecision() { return precision; }

    public Boolean getNullable() { return isNullable; }

    public Integer getPrimaryKeyPartNumber() { return pkPartNum; }

    public String getComment() { return comment; }

    public boolean isNumericType() { return isJdbcTypeNumeric(jdbcTypeCode); }

    public boolean isCharacterType() { return isJdbcTypeChar(jdbcTypeCode); }

    public static boolean isJdbcTypeNumeric(Integer jdbc_type)
    {
           if ( jdbc_type == null )
            return false;

        switch (jdbc_type)
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

    public static boolean isJdbcTypeChar(Integer jdbc_type)
    {
        if ( jdbc_type == null )
            return false;

        switch (jdbc_type)
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