/**
 * Copyright (C) 2009-2012 enStratus Networks Inc
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.rackspace;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;

public abstract class AbstractMethod {
    protected RackspaceCloud provider;
    
    public AbstractMethod(@Nonnull RackspaceCloud provider) { this.provider = provider; }
    
    public synchronized @Nullable AuthenticationContext authenticate() throws CloudException, InternalException {
        Logger std = RackspaceCloud.getLogger(RackspaceCloud.class, "std");
        Logger wire = RackspaceCloud.getLogger(RackspaceCloud.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".authenticate()");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("--------------------------------------------------------> " + provider.getEndpoint());
            wire.debug("");
        }
        try {
            ProviderContext ctx = provider.getContext();
            HttpClient client = getClient();
            HttpGet get = new HttpGet(provider.getEndpoint());
            
            try {
                get.addHeader("Content-Type", "application/json");
                get.addHeader("X-Auth-User", new String(ctx.getAccessPublic(), "utf-8"));
                get.addHeader("X-Auth-Key", new String(ctx.getAccessPrivate(), "utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                std.error("authenticate(): Unsupported encoding when building request headers: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new InternalException(e);
            }
            if( wire.isDebugEnabled() ) {
                wire.debug(get.getRequestLine().toString());
                for( Header header : get.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(get);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);
            if( code != HttpServletResponse.SC_NO_CONTENT ) {
                if( code == HttpServletResponse.SC_FORBIDDEN || code == HttpServletResponse.SC_UNAUTHORIZED ) {
                    return null;
                }
                std.error("authenticate(): Expected NO CONTENT for an authentication request, got " + code);
                HttpEntity entity = response.getEntity();
                String json = null;

                if( entity != null ) {
                    try {
                        json = EntityUtils.toString(entity);

                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
                }
                RackspaceException.ExceptionItems items;
                if( json == null ) {
                    items = RackspaceException.parseException(code, "{}");
                }
                else {
                    items = RackspaceException.parseException(code, json);
                }
                if( items.type.equals(CloudErrorType.AUTHENTICATION) ) {
                    return null;
                }
                std.error("authenticate(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            else {
                AuthenticationContext authContext = new AuthenticationContext();
                
                for( Header h : response.getAllHeaders() ) {
                    if( h.getName().equalsIgnoreCase("x-auth-token") ) {
                        authContext.setAuthToken(h.getValue().trim());
                    }
                    else if( h.getName().equalsIgnoreCase("x-server-management-url") ) {
                        authContext.setServerUrl(h.getValue().trim());
                    }
                    else if( h.getName().equalsIgnoreCase("x-storage-url") ) {
                        authContext.setStorageUrl(h.getValue().trim());
                    }
                    else if( h.getName().equalsIgnoreCase("x-cdn-management-url") ) {
                        authContext.setCdnUrl(h.getValue().trim());
                    }
                    else if( h.getName().equalsIgnoreCase("x-storage-token") ) {
                        authContext.setStorageToken(h.getValue().trim());
                    }
                }
                if( authContext.getAuthToken() == null ) {
                    std.warn("authenticate(): No authentication token in response");
                    throw new CloudException("No authentication token in cloud response");
                }
                return authContext;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".authenticate()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------> " + provider.getEndpoint());
            }            
        }
    }
    
    protected void delete(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        Logger std = RackspaceCloud.getLogger(RackspaceCloud.class, "std");
        Logger wire = RackspaceCloud.getLogger(RackspaceCloud.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".delete(" + authToken + "," + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("--------------------------------------------------------> " + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            HttpDelete delete = new HttpDelete(endpoint + resource);
            
            delete.addHeader("Content-Type", "application/json");
            delete.addHeader("X-Auth-Token", authToken);

            if( wire.isDebugEnabled() ) {
                wire.debug(delete.getRequestLine().toString());
                for( Header header : delete.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(delete);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            if( code != HttpServletResponse.SC_NO_CONTENT && code != HttpServletResponse.SC_ACCEPTED ) {
                std.error("delete(): Expected NO CONTENT for DELETE request, got " + code);
                HttpEntity entity = response.getEntity();
                String json = null;

                if( entity != null ) {
                    try {
                        json = EntityUtils.toString(entity);

                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
                }
                RackspaceException.ExceptionItems items = (json == null ? null : RackspaceException.parseException(code, json));
                
                if( items == null ) {
                    items = new RackspaceException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("delete(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            else {
                wire.debug("");
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".delete()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------> " + endpoint + resource);
            }               
        }
    }
    
    protected @Nullable String getString(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        Logger std = RackspaceCloud.getLogger(RackspaceCloud.class, "std");
        Logger wire = RackspaceCloud.getLogger(RackspaceCloud.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".getString(" + authToken + "," + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("--------------------------------------------------------> " + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            HttpGet get = new HttpGet(endpoint + resource);
            
            get.addHeader("Content-Type", "application/json");
            get.addHeader("X-Auth-Token", authToken);

            if( wire.isDebugEnabled() ) {
                wire.debug(get.getRequestLine().toString());
                for( Header header : get.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(get);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            if( code == HttpServletResponse.SC_NOT_FOUND ) {
                return null;
            }
            if( code != HttpServletResponse.SC_NO_CONTENT && code != HttpServletResponse.SC_OK && code != HttpServletResponse.SC_NON_AUTHORITATIVE_INFORMATION ) {
                std.error("getString(): Expected OK for GET request, got " + code);
                HttpEntity entity = response.getEntity();
                String json = null;

                if( entity != null ) {
                    try {
                        json = EntityUtils.toString(entity);

                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
                }

                RackspaceException.ExceptionItems items = (json == null ? null : RackspaceException.parseException(code, json));
                
                if( items == null ) {
                    return null;
                }
                std.error("getString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            else {
                HttpEntity entity = response.getEntity();
                String json = null;

                if( entity != null ) {
                    try {
                        json = EntityUtils.toString(entity);

                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
                }
                return json;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".getString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------> " + endpoint + resource);
            }               
        }
    }
    
    protected @Nullable InputStream getStream(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        Logger std = RackspaceCloud.getLogger(RackspaceCloud.class, "std");
        Logger wire = RackspaceCloud.getLogger(RackspaceCloud.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".getStream(" + authToken + "," + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("--------------------------------------------------------> " + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            HttpGet get = new HttpGet(endpoint + resource);
            
            get.addHeader("Content-Type", "application/json");
            get.addHeader("X-Auth-Token", authToken);

            if( wire.isDebugEnabled() ) {
                wire.debug(get.getRequestLine().toString());
                for( Header header : get.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(get);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            if( code == HttpServletResponse.SC_NOT_FOUND ) {
                return null;
            }
            if( code != HttpServletResponse.SC_OK && code != HttpServletResponse.SC_NON_AUTHORITATIVE_INFORMATION ) {
                std.error("getStream(): Expected OK for GET request, got " + code);
                HttpEntity entity = response.getEntity();
                String json = null;

                if( entity != null ) {
                    try {
                        json = EntityUtils.toString(entity);

                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
                }
                RackspaceException.ExceptionItems items = (json == null ? null : RackspaceException.parseException(code, json));
                
                if( items == null ) {
                    return null;
                }
                std.error("getStream(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            else {
                HttpEntity entity = response.getEntity();

                if( entity == null ) {
                    return null;
                }
                InputStream input;

                try {
                    input = entity.getContent();
                }
                catch( IOException e ) {
                    std.error("get(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug("---> Binary Data <---");
                    wire.debug("");
                }
                return input;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".getStream()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------> " + endpoint + resource);
            }               
        }
    }

    private @Nonnull HttpClient getClient() throws CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was provided for this request");
        }
        String endpoint = ctx.getEndpoint();
        boolean ssl = (endpoint != null && endpoint.startsWith("https"));
        HttpParams params = new BasicHttpParams();

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        //noinspection deprecation
        HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);
        HttpProtocolParams.setUserAgent(params, "Dasein Cloud");

        Properties p = ctx.getCustomProperties();

        if( p != null ) {
            String proxyHost = p.getProperty("proxyHost");
            String proxyPort = p.getProperty("proxyPort");

            if( proxyHost != null ) {
                int port = 0;

                if( proxyPort != null && proxyPort.length() > 0 ) {
                    port = Integer.parseInt(proxyPort);
                }
                params.setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(proxyHost, port, ssl ? "https" : "http"));
            }
        }
        return new DefaultHttpClient(params);
    }
    
    protected @Nullable Map<String,String> head(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        Logger std = RackspaceCloud.getLogger(RackspaceCloud.class, "std");
        Logger wire = RackspaceCloud.getLogger(RackspaceCloud.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".head(" + authToken + "," + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("--------------------------------------------------------> " + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            HttpHead head = new HttpHead(endpoint + resource);
            
            head.addHeader("X-Auth-Token", authToken);

            if( wire.isDebugEnabled() ) {
                wire.debug(head.getRequestLine().toString());
                for( Header header : head.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(head);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);


            if( code != HttpServletResponse.SC_NO_CONTENT && code != HttpServletResponse.SC_OK ) {
                if( code == HttpServletResponse.SC_NOT_FOUND ) {
                    return null;
                }
                std.error("head(): Expected NO CONTENT or OK for HEAD request, got " + code);
                HttpEntity entity = response.getEntity();
                String json = null;

                if( entity != null ) {
                    try {
                        json = EntityUtils.toString(entity);

                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
                }
                RackspaceException.ExceptionItems items = (json == null ? null : RackspaceException.parseException(code, json));
                
                if( items == null ) {
                    return null;
                }
                std.error("head(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            HashMap<String,String> map = new HashMap<String,String>();
            
            for( Header h : response.getAllHeaders() ) {
                map.put(h.getName().trim(), h.getValue().trim());
            }
            return map;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".head()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("--------------------------------------------------------> " + endpoint + resource);
            }               
        }
    }

    @SuppressWarnings("unused")
    protected @Nullable String postHeaders(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nonnull Map<String,String> customHeaders) throws CloudException, InternalException {
        Logger std = RackspaceCloud.getLogger(RackspaceCloud.class, "std");
        Logger wire = RackspaceCloud.getLogger(RackspaceCloud.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".postString(" + authToken + "," + endpoint + "," + resource + "," + customHeaders + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            HttpPost post = new HttpPost(endpoint + resource);
            
            post.addHeader("Content-Type", "application/json");
            post.addHeader("X-Auth-Token", authToken);
            if( customHeaders != null ) {
                for( Map.Entry<String, String> entry : customHeaders.entrySet() ) {
                    String val = (entry.getValue() == null ? "" : entry.getValue());
                    
                    post.addHeader(entry.getKey(), val);
                }
            }

            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            if( code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("postString(): Expected ACCEPTED for POST request, got " + code);
                HttpEntity entity = response.getEntity();
                String json = null;

                if( entity != null ) {
                    try {
                        json = EntityUtils.toString(entity);

                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
                }
                RackspaceException.ExceptionItems items = (json == null ? null : RackspaceException.parseException(code, json));
                
                if( items == null ) {
                    items = new RackspaceException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("postString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            else {
                if( code == HttpServletResponse.SC_ACCEPTED ) {
                    HttpEntity entity = response.getEntity();
                    String json = null;

                    if( entity != null ) {
                        try {
                            json = EntityUtils.toString(entity);

                            if( wire.isDebugEnabled() ) {
                                wire.debug(json);
                                wire.debug("");
                            }
                        }
                        catch( IOException e ) {
                            throw new CloudException(e);
                        }
                    }
                    if( json != null && !json.trim().equals("") ) {
                        return json;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".postString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }
    
    protected @Nullable String postString(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nullable String payload) throws CloudException, InternalException {
        Logger std = RackspaceCloud.getLogger(RackspaceCloud.class, "std");
        Logger wire = RackspaceCloud.getLogger(RackspaceCloud.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".postString(" + authToken + "," + endpoint + "," + resource + "," + payload + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            HttpPost post = new HttpPost(endpoint + resource);
            
            post.addHeader("Content-Type", "application/json");
            post.addHeader("X-Auth-Token", authToken);

            if( payload != null ) {
                post.setEntity(new StringEntity(payload, ContentType.APPLICATION_JSON));
            }
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                if( payload != null ) {
                    wire.debug(payload);
                    wire.debug("");
                }
            }
            HttpResponse response;

            try {
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            if( code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("postString(): Expected ACCEPTED for POST request, got " + code);
                HttpEntity entity = response.getEntity();
                String json = null;

                if( entity != null ) {
                    try {
                        json = EntityUtils.toString(entity);

                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
                }
                RackspaceException.ExceptionItems items = (json == null ? null : RackspaceException.parseException(code, json));

                if( items == null ) {
                    items = new RackspaceException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("postString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            else {
                if( code == HttpServletResponse.SC_ACCEPTED ) {
                    HttpEntity entity = response.getEntity();
                    String json = null;

                    if( entity != null ) {
                        try {
                            json = EntityUtils.toString(entity);

                            if( wire.isDebugEnabled() ) {
                                wire.debug(json);
                                wire.debug("");
                            }
                        }
                        catch( IOException e ) {
                            throw new CloudException(e);
                        }
                    }
                    if( json != null && !json.trim().equals("") ) {
                        return json;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".postString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }

    @SuppressWarnings("unused")
    protected @Nullable String postStream(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nullable String md5Hash, @Nullable InputStream stream) throws CloudException, InternalException {
        Logger std = RackspaceCloud.getLogger(RackspaceCloud.class, "std");
        Logger wire = RackspaceCloud.getLogger(RackspaceCloud.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".postStream(" + authToken + "," + endpoint + "," + resource + "," + md5Hash + ",INPUTSTREAM)");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            HttpPost post = new HttpPost(endpoint + resource);
            
            post.addHeader("Content-Type", "application/octet-stream");
            post.addHeader("X-Auth-Token", authToken);

            post.setEntity(new InputStreamEntity(stream, -1, ContentType.APPLICATION_OCTET_STREAM));

            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                wire.debug("--> BINARY DATA <--");
            }
            HttpResponse response;

            try {
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            String responseHash = null;
            
            for( Header h : response.getAllHeaders() ) {
                if( h.getName().equals("ETag") ) {
                    responseHash = h.getValue(); 
                }
            }
            if( responseHash != null && md5Hash != null && !responseHash.equals(md5Hash) ) {
                throw new CloudException("MD5 hash values do not match, probably data corruption");
            }
            if( code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("postStream(): Expected ACCEPTED or NO CONTENT for POST request, got " + code);
                HttpEntity entity = response.getEntity();
                String json = null;

                if( entity != null ) {
                    try {
                        json = EntityUtils.toString(entity);

                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
                }
                RackspaceException.ExceptionItems items = (json == null ? null : RackspaceException.parseException(code, json));

                if( items == null ) {
                    items = new RackspaceException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("postStream(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            else {
                wire.debug("");
                if( code == HttpServletResponse.SC_ACCEPTED ) {
                    HttpEntity entity = response.getEntity();
                    String json = null;

                    if( entity != null ) {
                        try {
                            json = EntityUtils.toString(entity);

                            if( wire.isDebugEnabled() ) {
                                wire.debug(json);
                                wire.debug("");
                            }
                        }
                        catch( IOException e ) {
                            throw new CloudException(e);
                        }
                    }
                    if( json != null && !json.trim().equals("") ) {
                        return json;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + RackspaceCloud.class.getName() + ".postStream()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }
    
    protected @Nullable String putHeaders(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nonnull Map<String,String> customHeaders) throws CloudException, InternalException {
        Logger std = RackspaceCloud.getLogger(RackspaceCloud.class, "std");
        Logger wire = RackspaceCloud.getLogger(RackspaceCloud.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".putHeaders(" + authToken + "," + endpoint + "," + resource + "," + customHeaders + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            HttpPut put = new HttpPut(endpoint + resource);
            
            put.addHeader("Content-Type", "application/json");
            put.addHeader("X-Auth-Token", authToken);
            if( customHeaders != null ) {
                for( Map.Entry<String, String> entry : customHeaders.entrySet() ) {
                    String val = (entry.getValue() == null ? "" : entry.getValue());
                    
                    put.addHeader(entry.getKey(), val);
                }
            }

            if( wire.isDebugEnabled() ) {
                wire.debug(put.getRequestLine().toString());
                for( Header header : put.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(put);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            if( code != HttpServletResponse.SC_CREATED && code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("putString(): Expected CREATED, ACCEPTED, or NO CONTENT for put request, got " + code);
                HttpEntity entity = response.getEntity();
                String json = null;

                if( entity != null ) {
                    try {
                        json = EntityUtils.toString(entity);

                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
                }
                RackspaceException.ExceptionItems items = (json == null ? null : RackspaceException.parseException(code, json));

                if( items == null ) {
                    items = new RackspaceException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("putString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            else {
                if( code == HttpServletResponse.SC_ACCEPTED || code == HttpServletResponse.SC_CREATED ) {
                    HttpEntity entity = response.getEntity();
                    String json = null;

                    if( entity != null ) {
                        try {
                            json = EntityUtils.toString(entity);

                            if( wire.isDebugEnabled() ) {
                                wire.debug(json);
                                wire.debug("");
                            }
                        }
                        catch( IOException e ) {
                            throw new CloudException(e);
                        }
                    }
                    if( json != null && !json.trim().equals("") ) {
                        return json;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".putString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }
    
    protected @Nullable String putString(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nullable String payload) throws CloudException, InternalException {
        Logger std = RackspaceCloud.getLogger(RackspaceCloud.class, "std");
        Logger wire = RackspaceCloud.getLogger(RackspaceCloud.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".putString(" + authToken + "," + endpoint + "," + resource + "," + payload + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            HttpPut put = new HttpPut(endpoint + resource);
            
            put.addHeader("Content-Type", "application/json");
            put.addHeader("X-Auth-Token", authToken);

            if( payload != null ) {
                put.setEntity(new StringEntity(payload, ContentType.APPLICATION_JSON));
            }
            if( wire.isDebugEnabled() ) {
                wire.debug(put.getRequestLine().toString());
                for( Header header : put.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                if( payload != null ) {
                    wire.debug(payload);
                    wire.debug("");
                }
            }
            HttpResponse response;

            try {
                response = client.execute(put);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            if( code != HttpServletResponse.SC_CREATED && code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("putString(): Expected CREATED, ACCEPTED, or NO CONTENT for put request, got " + code);
                HttpEntity entity = response.getEntity();
                String json = null;

                if( entity != null ) {
                    try {
                        json = EntityUtils.toString(entity);

                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
                }
                RackspaceException.ExceptionItems items = (json == null ? null : RackspaceException.parseException(code, json));

                if( items == null ) {
                    items = new RackspaceException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("putString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            else {
                if( code == HttpServletResponse.SC_ACCEPTED || code == HttpServletResponse.SC_CREATED ) {
                    HttpEntity entity = response.getEntity();
                    String json = null;

                    if( entity != null ) {
                        try {
                            json = EntityUtils.toString(entity);

                            if( wire.isDebugEnabled() ) {
                                wire.debug(json);
                                wire.debug("");
                            }
                        }
                        catch( IOException e ) {
                            throw new CloudException(e);
                        }
                    }
                    if( json != null && !json.trim().equals("") ) {
                        return json;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + AbstractMethod.class.getName() + ".putString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }
    
    protected @Nullable String putStream(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nullable String md5Hash, @Nullable InputStream stream) throws CloudException, InternalException {
        Logger std = RackspaceCloud.getLogger(RackspaceCloud.class, "std");
        Logger wire = RackspaceCloud.getLogger(RackspaceCloud.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + AbstractMethod.class.getName() + ".putStream(" + authToken + "," + endpoint + "," + resource + "," + md5Hash + ",INPUTSTREAM)");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            HttpPut put = new HttpPut(endpoint + resource);
            
            put.addHeader("Content-Type", "application/octet-stream");
            put.addHeader("X-Auth-Token", authToken);
            if( md5Hash != null ) {
                put.addHeader("ETag", md5Hash);
            }
            put.setEntity(new InputStreamEntity(stream, -1, ContentType.APPLICATION_OCTET_STREAM));

            if( wire.isDebugEnabled() ) {
                wire.debug(put.getRequestLine().toString());
                for( Header header : put.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                wire.debug("--> BINARY DATA <--");
            }
            HttpResponse response;

            try {
                response = client.execute(put);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                std.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            std.debug("HTTP STATUS: " + code);

            String responseHash = null;
            
            for( Header h : response.getAllHeaders() ) {
                if( h.getName().equals("ETag") ) {
                    responseHash = h.getValue(); 
                }
            }
            if( responseHash != null && md5Hash != null && !responseHash.equals(md5Hash) ) {
                throw new CloudException("MD5 hash values do not match, probably data corruption");
            }
            if( code != HttpServletResponse.SC_CREATED && code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("putStream(): Expected CREATED, ACCEPTED, or NO CONTENT for PUT request, got " + code);
                HttpEntity entity = response.getEntity();
                String json = null;

                if( entity != null ) {
                    try {
                        json = EntityUtils.toString(entity);

                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                    catch( IOException e ) {
                        throw new CloudException(e);
                    }
                }
                RackspaceException.ExceptionItems items = (json == null ? null : RackspaceException.parseException(code, json));

                if( items == null ) {
                    items = new RackspaceException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("putStream(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            else {
                if( code == HttpServletResponse.SC_ACCEPTED ) {
                    HttpEntity entity = response.getEntity();
                    String json = null;

                    if( entity != null ) {
                        try {
                            json = EntityUtils.toString(entity);

                            if( wire.isDebugEnabled() ) {
                                wire.debug(json);
                                wire.debug("");
                            }
                        }
                        catch( IOException e ) {
                            throw new CloudException(e);
                        }
                    }
                    if( json != null && !json.trim().equals("") ) {
                        return json;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + RackspaceCloud.class.getName() + ".putStream()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }
}
