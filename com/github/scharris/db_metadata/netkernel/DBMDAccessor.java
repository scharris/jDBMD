package com.github.scharris.db_metadata.netkernel;

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


import com.github.scharris.db_metadata.*;
import com.github.scharris.db_metadata.DBMetaData.CaseSensitivity;
import com.github.scharris.db_metadata.RelationMetaData.RelationType;

import com.ten60.netkernel.urii.aspect.*;
import org.ten60.netkernel.layer1.nkf.*;
import org.ten60.netkernel.layer1.nkf.impl.NKFAccessorImpl;
import org.ten60.netkernel.xml.representation.DOMXDAAspect;
import org.ten60.netkernel.xml.xda.DOMXDA;
import com.ten60.netkernel.util.NetKernelException;
import org.ten60.rdbms.representation.*;

import java.sql.*;
import java.util.*;

import org.w3c.dom.*;

import javax.xml.parsers.*;



public class DBMDAccessor extends NKFAccessorImpl {
    
    
    public static final String ARG_DBCONFIG = "dbconfig";

    public static final String DEFAULT_DBCONFIG = "ffcpl:/etc/ConfigRDBMS.xml";

    
    public DBMDAccessor()
    {
        super(SAFE_FOR_CONCURRENT_USE, INKFRequestReadOnly.RQT_SOURCE);
    }

    
    public void processRequest(INKFConvenienceHelper context) throws Exception
    {
        INKFRequestReadOnly req = context.getThisRequest();

        String schema_arg = req.getArgument("schema");
        String schema = schema_arg != null && schema_arg.indexOf(':') == -1 ? schema_arg
            : ((IAspectString)context.sourceAspect("this:param:schema", IAspectString.class)).getString();
        
        Connection conn = null;
        
        IAspectDBConnectionPool connPool = (IAspectDBConnectionPool)context.sourceAspect(getDbConfigURI(req),
                                                                                         IAspectDBConnectionPool.class);
        try
        {
            conn = connPool.acquireConnection();
            
            DatabaseMetaData dbmd = conn.getMetaData();
            
            CaseSensitivity case_sens = DBMetaData.getDbCaseSensitivity(dbmd);
            
            schema = DBMetaData.normalizeDatabaseIdentifier(schema, case_sens);
            
            
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            
            Element root_el = (Element)doc.appendChild(doc.createElement("database-metadata"));
            if ( schema != null )
                root_el.setAttribute("schema", schema); 
            root_el.setAttribute("identifiers-case-sensitivity", case_sens.toString().toLowerCase());
            
            
            Node rels_el = root_el.appendChild(doc.createElement("relations"));
            
            Map<RelationID,RelationType> included_rel_ids_and_types = 
                DBMetaData.fetchRelationIDsAndTypes(dbmd, 
                                                    schema,
                                                    getOption("tables", context, true),
                                                    getOption("views",  context, false));
            
            if ( getOption("fields", context, false) ) // include fields?
            {
                for(RelationMetaData rel_md: DBMetaData.fetchRelationMetaDatas(included_rel_ids_and_types, schema, dbmd))
                    rels_el.appendChild(makeFullRelationElement(rel_md, doc));
            }
            else
            {
                for(Map.Entry<RelationID,RelationType> rid_rtype: included_rel_ids_and_types.entrySet())
                    rels_el.appendChild(makeChildlessRelationElement(rid_rtype.getKey(), rid_rtype.getValue(), doc));
            }
            
            // Foreign Key Links
            if (getOption("fks", context, false))
            {
                Element fk_links_el = (Element)root_el.appendChild(doc.createElement("foreign-key-links"));
                
                for(FkLink fkl: DBMetaData.fetchForeignKeyLinks(schema, dbmd))
                    fk_links_el.appendChild(makeForeignKeyLinkElement(fkl, doc));
            }
            

            DOMXDAAspect domxda_aspect = new DOMXDAAspect(new DOMXDA(doc));
            
            INKFResponse response = context.createResponseFrom(domxda_aspect);

            response.setMimeType("application/xml");

            response.setCacheable();
        }
        catch (NetKernelException e)
        {
            NKFException e2 = new NKFException(e.getId(), e.getMessage(), null);
            if (e.getCause() != null)
            {
                e2.addCause(e.getCause());
            }
            throw e2;
        }
        finally
        {
            if (conn != null)
            {
                connPool.releaseConnection(conn);
            }

        }
    }
    
    
    public Element makeChildlessRelationElement(RelationID rel_id, RelationType rel_type, Document doc)
    {
        Element rel_el = doc.createElement(rel_type.toString().toLowerCase());
                
        rel_el.setAttribute("name", rel_id.name());
                
        if ( rel_id.schema() != null )
            rel_el.setAttribute("schema", rel_id.schema());
                
        if ( rel_id.catalog() != null )
            rel_el.setAttribute("catalog", rel_id.catalog());
                
        rel_el.setAttribute("id", "r:" + rel_id.id());
        
        return rel_el;
    }
    
    
    public Element makeFullRelationElement(RelationMetaData rel_md, Document doc)
    {
        Element rel_el = makeChildlessRelationElement(rel_md.relationID(), rel_md.relationType(), doc);
        
        for(Field f: rel_md.fields())
            rel_el.appendChild(makeFieldElement(f, doc));
        
        return rel_el;
    }
    
    protected Element makeFieldElement(Field f, Document doc)
    {
        Element field_el = doc.createElement("field");
        
        field_el.setAttribute("name", f.name());
        
        RelationID rel_id = f.relationID();
        field_el.setAttribute("id", "f:" + rel_id.id() + "." + f.name().toLowerCase());

        Element type_el = doc.createElement("type");
        
        appendChildWithText(doc, type_el, "database-type", f.dbTypeName());
        appendChildWithText(doc, type_el, "jdbc-type-code", String.valueOf(f.jdbcTypeCode())); 
        appendChildWithText(doc, type_el, "jdbc-type-text", DBMetaData.jdbcTypeToString(f.jdbcTypeCode())); 
        if ( f.length() != null )
            appendChildWithText(doc, type_el, "max-chars", String.valueOf(f.length()));
        if ( f.precision() != null )
            appendChildWithText(doc, type_el, "precision", String.valueOf(f.precision()));
        if ( f.fractionalDigits() != null )
            appendChildWithText(doc, type_el, "scale", String.valueOf(f.fractionalDigits()));
        if ( f.radix() != null )
            appendChildWithText(doc, type_el, "radix", String.valueOf(f.radix()));
        if ( f.comment() != null )
            appendChildWithText(doc, type_el, "comment", String.valueOf(f.comment()));
        
        field_el.appendChild(type_el);
        
        appendChildWithText(doc, field_el, "nullable", (f.isNullable() == null ? "unknown" : f.isNullable().toString()));
        
        if ( f.pkPartNum() != null )
            appendChildWithText(doc, field_el, "primary-key-part", String.valueOf(f.pkPartNum()));
        
        return field_el;
    }
    
    
    public Element makeForeignKeyLinkElement(FkLink l, Document doc)
    {
        Element link_el = doc.createElement("link");
       
        Element src_rel_el = (Element)link_el.appendChild(doc.createElement("referencing-relation"));

        src_rel_el.setAttribute("name", l.srcRel().name());

        if (l.srcRel().schema() != null)
            src_rel_el.setAttribute("schema", l.srcRel().schema());

        if (l.srcRel().catalog() != null)
            src_rel_el.setAttribute("catalog", l.srcRel().catalog());

        
        Element tgt_rel_el = (Element)link_el.appendChild(doc.createElement("referenced-relation"));
        
        tgt_rel_el.setAttribute("name", l.tgtRel().name());

        if (l.tgtRel().schema() != null)
            tgt_rel_el.setAttribute("schema", l.tgtRel().schema());

        if (l.tgtRel().catalog() != null)
            tgt_rel_el.setAttribute("catalog", l.tgtRel().catalog());
        

        for(FkComp comp: l.fkComps())
        {
            Element match_el = (Element)link_el.appendChild(doc.createElement("match"));
            match_el.setAttribute("fk-field", comp.fkFieldName());
            match_el.setAttribute("pk-field", comp.pkFieldName());
        }

       return link_el;
    }
    
    
    protected String getDbConfigURI(INKFRequestReadOnly req)
    {
        if (req.argumentExists(ARG_DBCONFIG))
            return "this:param:" + ARG_DBCONFIG;
        else
            return DEFAULT_DBCONFIG;
    }
    
    
    protected boolean getOption(String option, INKFConvenienceHelper context, boolean def) throws NKFException
    {
        if ( context.getThisRequest().argumentExists(option) )
        {
            // Check for a simple non-uri parameter value as a convenience
            String arg_lower = context.getThisRequest().getArgument(option).toLowerCase();

            if ( arg_lower.equals("y") || arg_lower.equals("yes") || arg_lower.equals("t") || arg_lower.equals("true") )
                return true;
            else if ( arg_lower.equals("n") || arg_lower.equals("no") || arg_lower.equals("f") || arg_lower.equals("false") )
                return false;
            else
            {    
                String optval_str = ((IAspectString)context.sourceAspect("this:param:" + option, 
                                                                         IAspectString.class)).getString().toLowerCase();

                if ( optval_str.equals("y") || optval_str.equals("yes") || optval_str.equals("t") || optval_str.equals("true") )
                    return true;
                else
                    return false;
            }
        }
        else
            return def;
    }
    
    protected static void appendChildWithText(Document doc, Node node, String child_name, String child_text)
    {
        Element child = doc.createElement(child_name);
        child.setTextContent(child_text);
        node.appendChild(child);
    }
    
}






