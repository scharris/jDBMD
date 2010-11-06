package gov.fda.nctr.dbmd;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.FIELD)
public class Field {

	@XmlElement(name="rel-id")
    RelId relId;
    
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

    public Field(RelId relId,
                 String name,
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
        this.relId = relId;
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

    public RelId getRelationId()
    {
        return relId;
    }


    public String getName()
    {
        return name;
    }


    public int getJDBCTypeCode()
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

}