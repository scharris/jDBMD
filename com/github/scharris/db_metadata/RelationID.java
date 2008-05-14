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


public class RelationID {
    
    String catalog;
    String schema;
    String name;
    
    public RelationID(String catalog, String schema, String name)
    {
        this.catalog = catalog;
        this.schema = schema;
        this.name = name;
    }
    
    public String id()
    {
        return ((catalog != null ? "[" + catalog + "]" : "") +
               (schema != null ? schema + "." : "") +
               name).toLowerCase();
    }
    
    public String catalog()
    {
        return catalog;
    }
    public String schema()
    {
        return schema;
    }
    public String name()
    {
        return name;
    }
    
    public boolean equals(Object other)
    {
        if ( !(other instanceof RelationID) )
            return false;
        else
        {
            RelationID o = (RelationID)other;
            return eq(catalog, o.catalog) &&
                   eq(schema, o.schema) &&
                   eq(name, o.name);
        }
    }
    
    public int hashCode()
    {
        return (catalog != null ? catalog.hashCode() : 0) +
               (schema != null ? schema.hashCode() : 0) +
               name.hashCode();
    }
    
    static boolean eq(Object o1, Object o2)
    {
        return (o1 == null && o2 == null) || o1.equals(o2);
    }
}
