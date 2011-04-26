package gov.fda.nctr.dbmd;

import java.sql.Types;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

@XmlAccessorType(XmlAccessType.FIELD)
public class Field {

    @XmlAttribute
    String name;

    @XmlAttribute(name="jdbc-type-code")
    int jdbcTypeCode;

    @XmlAttribute(name="db-type-name")
    String dbTypeName;

    @XmlAttribute
    Integer length;

    @XmlAttribute
    Integer precision;

    @XmlAttribute(name="fractional-digits")
    Integer fractionalDigits;

    @XmlAttribute
    Integer radix;

    @XmlAttribute(name="nullable")
    Boolean isNullable;

    @XmlAttribute(name="pk-part-num")
    Integer pkPartNum;

    String comment;

    public Field(String name,
                 int jdbcTypeCode,
                 String dbTypeName,
                 Integer length,
                 Integer precision,
                 Integer fractionalDigits,
                 Integer radix,
                 Boolean isNullable,
                 Integer pkPartNum,
                 String comment)
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

    public String getName()
    {
        return name;
    }


    public int getJdbcTypeCode()
    {
        return jdbcTypeCode;
    }

    public String getDatabaseType()
    {
        return dbTypeName;
    }

    public Integer getLength()
    {
        return length;
    }

    public Integer getFractionalDigits()
    {
        return fractionalDigits;
    }

    public Integer getRadix()
    {
        return radix;
    }

    public Integer getPrecision()
    {
        return precision;
    }

    public Boolean getNullable()
    {
        return isNullable;
    }

    public Integer getPrimaryKeyPartNumber()
    {
        return pkPartNum;
    }

    public String getComment()
    {
        return comment;
    }

    public boolean isNumericType()
    {
        return isJdbcTypeNumeric(jdbcTypeCode);
    }

    public boolean isCharacterType()
    {
        return isJdbcTypeChar(jdbcTypeCode);
    }

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