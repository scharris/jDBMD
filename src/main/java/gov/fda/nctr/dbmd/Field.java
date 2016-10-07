package gov.fda.nctr.dbmd;

import java.io.Serializable;
import java.sql.Types;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

@XmlAccessorType(XmlAccessType.FIELD)
public class Field implements Serializable {

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

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((comment == null) ? 0 : comment.hashCode());
        result = prime * result + ((dbTypeName == null) ? 0 : dbTypeName.hashCode());
        result = prime * result + ((fractionalDigits == null) ? 0 : fractionalDigits.hashCode());
        result = prime * result + ((isNullable == null) ? 0 : isNullable.hashCode());
        result = prime * result + jdbcTypeCode;
        result = prime * result + ((length == null) ? 0 : length.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((pkPartNum == null) ? 0 : pkPartNum.hashCode());
        result = prime * result + ((precision == null) ? 0 : precision.hashCode());
        result = prime * result + ((radix == null) ? 0 : radix.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Field other = (Field) obj;
        if (comment == null)
        {
            if (other.comment != null)
                return false;
        }
        else if (!comment.equals(other.comment))
            return false;
        if (dbTypeName == null)
        {
            if (other.dbTypeName != null)
                return false;
        }
        else if (!dbTypeName.equals(other.dbTypeName))
            return false;
        if (fractionalDigits == null)
        {
            if (other.fractionalDigits != null)
                return false;
        }
        else if (!fractionalDigits.equals(other.fractionalDigits))
            return false;
        if (isNullable == null)
        {
            if (other.isNullable != null)
                return false;
        }
        else if (!isNullable.equals(other.isNullable))
            return false;
        if (jdbcTypeCode != other.jdbcTypeCode)
            return false;
        if (length == null)
        {
            if (other.length != null)
                return false;
        }
        else if (!length.equals(other.length))
            return false;
        if (name == null)
        {
            if (other.name != null)
                return false;
        }
        else if (!name.equals(other.name))
            return false;
        if (pkPartNum == null)
        {
            if (other.pkPartNum != null)
                return false;
        }
        else if (!pkPartNum.equals(other.pkPartNum))
            return false;
        if (precision == null)
        {
            if (other.precision != null)
                return false;
        }
        else if (!precision.equals(other.precision))
            return false;
        if (radix == null)
        {
            if (other.radix != null)
                return false;
        }
        else if (!radix.equals(other.radix))
            return false;
        return true;
    }

    private static final long serialVersionUID = 1L;
}