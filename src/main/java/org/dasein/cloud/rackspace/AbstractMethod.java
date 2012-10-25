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

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
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
            GetMethod get = new GetMethod(provider.getEndpoint());
            
            try {
                get.addRequestHeader("Content-Type", "application/json");
                get.addRequestHeader("X-Auth-User", new String(ctx.getAccessPublic(), "utf-8"));
                get.addRequestHeader("X-Auth-Key", new String(ctx.getAccessPrivate(), "utf-8"));
            }
            catch( UnsupportedEncodingException e ) {
                std.error("authenticate(): Unsupported encoding when building request headers: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new InternalException(e);
            }
            get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("GET " + get.getPath());
                for( Header header : get.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(get);
            }
            catch( IOException e ) {
                std.error("authenticate(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("authenticate(): HTTP Status " + code);
            }
            Header[] headers = get.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(get.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
            }
            if( code != HttpServletResponse.SC_NO_CONTENT ) {
                if( code == HttpServletResponse.SC_FORBIDDEN || code == HttpServletResponse.SC_UNAUTHORIZED ) {
                    return null;
                }
                std.error("authenticate(): Expected NO CONTENT for an authentication request, got " + code);
                String response;
                
                try {
                    response = get.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("authenticate(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                RackspaceException.ExceptionItems items = RackspaceException.parseException(code, response);
                
                if( items.type.equals(CloudErrorType.AUTHENTICATION) ) {
                    return null;
                }
                std.error("authenticate(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            else {
                AuthenticationContext authContext = new AuthenticationContext();
                
                for( Header h : headers ) {
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
            DeleteMethod delete = new DeleteMethod(endpoint + resource);
            
            delete.addRequestHeader("Content-Type", "application/json");
            delete.addRequestHeader("X-Auth-Token", authToken);
            delete.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("DELETE " + delete.getPath());
                for( Header header : delete.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(delete);
            }
            catch( IOException e ) {
                std.error("delete(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("delete(): HTTP Status " + code);
            }
            Header[] headers = delete.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(delete.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");                                
            }
            if( code != HttpServletResponse.SC_NO_CONTENT && code != HttpServletResponse.SC_ACCEPTED ) {
                std.error("delete(): Expected NO CONTENT for DELETE request, got " + code);
                String response;
                
                try {
                    response = delete.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("delete(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                RackspaceException.ExceptionItems items = RackspaceException.parseException(code, response);
                
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
            GetMethod get = new GetMethod(endpoint + resource);
            
            get.addRequestHeader("Content-Type", "application/json");
            get.addRequestHeader("X-Auth-Token", authToken);
            get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("GET " + get.getPath());
                for( Header header : get.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(get);
            }
            catch( IOException e ) {
                std.error("getString(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("getString(): HTTP Status " + code);
            }
            Header[] headers = get.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(get.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");                
            }
            if( code == HttpServletResponse.SC_NOT_FOUND ) {
                return null;
            }
            if( code != HttpServletResponse.SC_NO_CONTENT && code != HttpServletResponse.SC_OK && code != HttpServletResponse.SC_NON_AUTHORITATIVE_INFORMATION ) {
                std.error("getString(): Expected OK for GET request, got " + code);
                String response;
                
                try {
                    response = get.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("getString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                RackspaceException.ExceptionItems items = RackspaceException.parseException(code, response);
                
                if( items == null ) {
                    return null;
                }
                std.error("getString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            else {
                String response;
                
                try {
                    response = get.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("getString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                return response;
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
            GetMethod get = new GetMethod(endpoint + resource);
            
            get.addRequestHeader("Content-Type", "application/json");
            get.addRequestHeader("X-Auth-Token", authToken);
            get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("GET " + get.getPath());
                for( Header header : get.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(get);
            }
            catch( IOException e ) {
                std.error("getStream(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("getStream(): HTTP Status " + code);
            }
            Header[] headers = get.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(get.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");                
            }
            if( code == HttpServletResponse.SC_NOT_FOUND ) {
                return null;
            }
            if( code != HttpServletResponse.SC_OK && code != HttpServletResponse.SC_NON_AUTHORITATIVE_INFORMATION ) {
                std.error("getStream(): Expected OK for GET request, got " + code);
                String response;
                
                try {
                    response = get.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("getStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                RackspaceException.ExceptionItems items = RackspaceException.parseException(code, response);
                
                if( items == null ) {
                    return null;
                }
                std.error("getStream(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            else {
                InputStream input;
                
                try {
                    input = get.getResponseBodyAsStream();
                }
                catch( IOException e ) {
                    std.error("getStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug("---> Binary Data <---");
                }
                wire.debug("");
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

    protected @Nonnull HttpClient getClient() {
        ProviderContext ctx = provider.getContext();
        HttpClient client = new HttpClient();

        if( ctx != null ) {
            Properties p = ctx.getCustomProperties();

            if( p != null ) {
                String proxyHost = p.getProperty("proxyHost");
                String proxyPort = p.getProperty("proxyPort");

                if( proxyHost != null ) {
                    int port = 0;

                    if( proxyPort != null && proxyPort.length() > 0 ) {
                        port = Integer.parseInt(proxyPort);
                    }
                    client.getHostConfiguration().setProxy(proxyHost, port);
                }
            }
        }
        return client;
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
            HeadMethod head = new HeadMethod(endpoint + resource);
            
            head.addRequestHeader("X-Auth-Token", authToken);
            head.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("HEAD " + head.getPath());
                for( Header header : head.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(head);
            }
            catch( IOException e ) {
                std.error("head(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("head(): HTTP Status " + code);
            }
            Header[] headers = head.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(head.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");                                
            }
            if( code != HttpServletResponse.SC_NO_CONTENT && code != HttpServletResponse.SC_OK ) {
                if( code == HttpServletResponse.SC_NOT_FOUND ) {
                    return null;
                }
                std.error("head(): Expected NO CONTENT or OK for HEAD request, got " + code);
                String response;
                
                try {
                    response = head.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("head(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                RackspaceException.ExceptionItems items = RackspaceException.parseException(code, response);
                
                if( items == null ) {
                    return null;
                }
                std.error("head(): [" +  code + " : " + items.message + "] " + items.details);
                throw new RackspaceException(items);
            }
            HashMap<String,String> map = new HashMap<String,String>();
            
            for( Header h : headers ) {
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
            PostMethod post = new PostMethod(endpoint + resource);
            
            post.addRequestHeader("Content-Type", "application/json");
            post.addRequestHeader("X-Auth-Token", authToken);
            if( customHeaders != null ) {
                for( Map.Entry<String, String> entry : customHeaders.entrySet() ) {
                    String val = (entry.getValue() == null ? "" : entry.getValue());
                    
                    post.addRequestHeader(entry.getKey(), val);
                }
            }
            post.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("POST " + post.getPath());
                for( Header header : post.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                
            }
            int code;
            
            try {
                code = client.executeMethod(post);
            }
            catch( IOException e ) {
                std.error("postString(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("postString(): HTTP Status " + code);
            }
            Header[] headers = post.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            if( code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("postString(): Expected ACCEPTED for POST request, got " + code);
                String response;
                
                try {
                    response = post.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("postString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                RackspaceException.ExceptionItems items = RackspaceException.parseException(code, response);
                
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
                    String response;
                    
                    try {
                        response = post.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("postString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                        }
                        wire.debug("");
                        return response;
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
            PostMethod post = new PostMethod(endpoint + resource);
            
            post.addRequestHeader("Content-Type", "application/json");
            post.addRequestHeader("X-Auth-Token", authToken);
            post.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("POST " + post.getPath());
                for( Header header : post.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                
            }
            if( payload != null ) { 
                wire.debug(payload);
                wire.debug("");
                try {
                    post.setRequestEntity(new StringRequestEntity(payload, "application/json", "utf-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    std.error("postString(): UTF-8 is not supported locally: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new InternalException(e);
                }
            }
            int code;
            
            try {
                code = client.executeMethod(post);
            }
            catch( IOException e ) {
                std.error("postString(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("postString(): HTTP Status " + code);
            }
            Header[] headers = post.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            if( code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("postString(): Expected ACCEPTED for POST request, got " + code);
                String response;
                
                try {
                    response = post.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("postString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                RackspaceException.ExceptionItems items = RackspaceException.parseException(code, response);
                
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
                    String response;
                    
                    try {
                        response = post.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("postString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                        }
                        wire.debug("");
                        return response;
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
            PostMethod post = new PostMethod(endpoint + resource);
            
            post.addRequestHeader("Content-Type", "application/octet-stream");
            post.addRequestHeader("X-Auth-Token", authToken);
            post.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("POST " + post.getPath());
                for( Header header : post.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                
            }
            wire.debug("---> BINARY DATA <---");
            wire.debug("");
            post.setRequestEntity(new InputStreamRequestEntity(stream, "application/octet-stream"));
            int code;
            
            try {
                code = client.executeMethod(post);
            }
            catch( IOException e ) {
                std.error("postStream(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("postStream(): HTTP Status " + code);
            }
            Header[] headers = post.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            String responseHash = null;
            
            for( Header h : post.getResponseHeaders() ) {
                if( h.getName().equals("ETag") ) {
                    responseHash = h.getValue(); 
                }
            }
            if( responseHash != null && md5Hash != null && !responseHash.equals(md5Hash) ) {
                throw new CloudException("MD5 hash values do not match, probably data corruption");
            }
            if( code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("postStream(): Expected ACCEPTED or NO CONTENT for POST request, got " + code);
                String response;
                
                try {
                    response = post.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("postStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                RackspaceException.ExceptionItems items = RackspaceException.parseException(code, response);
                
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
                    String response;
                    
                    try {
                        response = post.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("postStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                        }
                        wire.debug("");
                        return response;
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
            PutMethod put = new PutMethod(endpoint + resource);
            
            put.addRequestHeader("Content-Type", "application/json");
            put.addRequestHeader("X-Auth-Token", authToken);
            if( customHeaders != null ) {
                for( Map.Entry<String, String> entry : customHeaders.entrySet() ) {
                    String val = (entry.getValue() == null ? "" : entry.getValue());
                    
                    put.addRequestHeader(entry.getKey(), val);
                }
            }
            put.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("PUT " + put.getPath());
                for( Header header : put.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(put);
            }
            catch( IOException e ) {
                std.error("putString(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("putString(): HTTP Status " + code);
            }
            Header[] headers = put.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(put.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            if( code != HttpServletResponse.SC_CREATED && code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("putString(): Expected CREATED, ACCEPTED, or NO CONTENT for put request, got " + code);
                String response;
                
                try {
                    response = put.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("putString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                RackspaceException.ExceptionItems items = RackspaceException.parseException(code, response);
                
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
                    String response;
                    
                    try {
                        response = put.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("putString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                            wire.debug("");
                        }
                        return response;
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
            PutMethod put = new PutMethod(endpoint + resource);
            
            put.addRequestHeader("Content-Type", "application/json");
            put.addRequestHeader("X-Auth-Token", authToken);
            put.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("PUT " + put.getPath());
                for( Header header : put.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            if( payload != null ) { 
                wire.debug(payload);
                wire.debug("");
                try {
                    put.setRequestEntity(new StringRequestEntity(payload, "application/json", "utf-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    std.error("putString(): UTF-8 is not supported locally: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new InternalException(e);
                }
            }
            int code;
            
            try {
                code = client.executeMethod(put);
            }
            catch( IOException e ) {
                std.error("putString(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("putString(): HTTP Status " + code);
            }
            Header[] headers = put.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(put.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            if( code != HttpServletResponse.SC_CREATED && code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("putString(): Expected CREATED, ACCEPTED, or NO CONTENT for put request, got " + code);
                String response;
                
                try {
                    response = put.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("putString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                RackspaceException.ExceptionItems items = RackspaceException.parseException(code, response);
                
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
                    String response;
                    
                    try {
                        response = put.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("putString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                            wire.debug("");
                        }
                        return response;
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
            PutMethod put = new PutMethod(endpoint + resource);
            
            put.addRequestHeader("Content-Type", "application/octet-stream");
            put.addRequestHeader("X-Auth-Token", authToken);
            if( md5Hash != null ) {
                put.addRequestHeader("ETag", md5Hash);
            }
            put.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("PUT " + put.getPath());
                for( Header header : put.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                wire.debug("---> BINARY DATA <---");
                wire.debug("");                
            }

            put.setRequestEntity(new InputStreamRequestEntity(stream, "application/octet-stream"));
            int code;
            
            try {
                code = client.executeMethod(put);
            }
            catch( IOException e ) {
                std.error("putStream(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("putStream(): HTTP Status " + code);
            }
            Header[] headers = put.getResponseHeaders();

            if( wire.isDebugEnabled() ) {
                wire.debug(put.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            String responseHash = null;
            
            for( Header h : put.getResponseHeaders() ) {
                if( h.getName().equals("ETag") ) {
                    responseHash = h.getValue(); 
                }
            }
            if( responseHash != null && md5Hash != null && !responseHash.equals(md5Hash) ) {
                throw new CloudException("MD5 hash values do not match, probably data corruption");
            }
            if( code != HttpServletResponse.SC_CREATED && code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("putStream(): Expected CREATED, ACCEPTED, or NO CONTENT for PUT request, got " + code);
                String response;
                
                try {
                    response = put.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("putStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                RackspaceException.ExceptionItems items = RackspaceException.parseException(code, response);
                
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
                    String response;
                    
                    try {
                        response = put.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("putStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                            wire.debug("");
                        }
                        return response;
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
