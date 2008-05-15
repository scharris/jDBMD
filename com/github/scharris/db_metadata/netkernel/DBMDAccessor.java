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


import java.sql.Connection;
import org.w3c.dom.Document;

import com.ten60.netkernel.urii.aspect.*;
import org.ten60.netkernel.layer1.nkf.*;
import org.ten60.netkernel.layer1.nkf.impl.NKFAccessorImpl;
import org.ten60.netkernel.xml.representation.DOMXDAAspect;
import org.ten60.netkernel.xml.xda.DOMXDA;
import com.ten60.netkernel.util.NetKernelException;
import org.ten60.rdbms.representation.*;

import com.github.scharris.db_metadata.DBMetaData;



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
            
            Document doc = (new DBMetaData()).createMetaDataDOM(conn.getMetaData(),
                                                                schema,
                                                                getOption("tables", context, true),
                                                                getOption("views",  context, false),
                                                                getOption("fields", context, false),
                                                                getOption("fks", context, false));
            

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
    
}






