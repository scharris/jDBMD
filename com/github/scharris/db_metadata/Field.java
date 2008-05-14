/* author: Stephen C. Harris.  Send comments, questions and fixes to gmail.com user steveOfAR.
 * Copyright (c) 2008 Stephen C. Harris
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *  
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *  
 *     * Neither the name of author nor the names of its contributors
 *       may be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *  
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.scharris.db_metadata;


public class Field {

    RelationID relID;
    
    String name;

    int jdbcTypeCode;
    
    String dbTypeName;

    Integer length;
    
    Integer precision;
    
    Integer fractionalDigits;
    
    Integer radix;
    
    Boolean isNullable;

    Integer pkPartNum;
    
    String comment;

    public Field(RelationID relID,
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
        this.relID = relID;
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

    public RelationID relationID()
    {
        return relID;
    }

    public String name()
    {
        return name;
    }

    public int jdbcTypeCode()
    {
        return jdbcTypeCode;
    }

    public String dbTypeName()
    {
        return dbTypeName;
    }

    public Integer length()
    {
        return length;
    }

    public Integer fractionalDigits()
    {
        return fractionalDigits;
    }

    public Integer radix()
    {
        return radix;
    }

    public Integer precision()
    {
        return precision;
    }

    public Boolean isNullable()
    {
        return isNullable;
    }

    public Integer pkPartNum()
    {
        return pkPartNum;
    }

    public String comment()
    {
        return comment;
    }

}