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

import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.rackspace.compute.RackspaceComputeServices;
import org.dasein.cloud.rackspace.network.RackspaceNetworkServices;
import org.dasein.cloud.rackspace.platform.RackspacePlatformServices;
import org.dasein.cloud.rackspace.storage.RackspaceStorageServices;

public class RackspaceCloud extends AbstractCloud {
    static private String getLastItem(String name) {
        int idx = name.lastIndexOf('.');
        
        if( idx < 0 ) {
            return name;
        }
        else if( idx == (name.length()-1) ) {
            return "";
        }
        return name.substring(idx+1);
    }
    
    static public Logger getLogger(Class<?> cls, String type) {
        String pkg = getLastItem(cls.getPackage().getName());
        
        if( pkg.equals("rackspace") ) {
            pkg = "";
        }
        else {
            pkg = pkg + ".";
        }
        return Logger.getLogger("dasein.cloud.rackspace." + type + "." + pkg + getLastItem(cls.getName()));
    }
    
    private AuthenticationContext authenticationContext;
    
    public RackspaceCloud() { }
    
    public synchronized AuthenticationContext getAuthenticationContext() throws CloudException, InternalException {
        if( authenticationContext == null ) {
            RackspaceMethod method = new RackspaceMethod(this);
            
            authenticationContext = method.authenticate();
            if( authenticationContext == null ) {
                RackspaceException.ExceptionItems items = new RackspaceException.ExceptionItems();
                
                items.code = HttpServletResponse.SC_UNAUTHORIZED;
                items.type = CloudErrorType.AUTHENTICATION;
                items.message = "unauthorized";
                items.details = "The API keys failed to authentication with the specified endpoint.";
                throw new RackspaceException(items);
            }
        }
        return authenticationContext;
    }
    
    @Override
    public @Nonnull String getCloudName() {
        return "Rackspace Cloud";
    }

    @Override
    public @Nonnull RackspaceComputeServices getComputeServices() {
        return new RackspaceComputeServices(this);
    }
    
    @Override
    public @Nonnull RackspaceLocationServices getDataCenterServices() {
        return new RackspaceLocationServices(this);
    }
    
    public @Nonnull String getEndpoint() {
        ProviderContext ctx = getContext();
        
        if( ctx == null ) {
            return "https://auth.api.rackspacecloud.com/v1.0";            
        }
        String endpoint = ctx.getEndpoint();

        if( endpoint == null || endpoint.trim().equals("") ) {
            return "https://auth.api.rackspacecloud.com/v1.0";
        }
        else {
            while( endpoint.endsWith("/") && !endpoint.equals("/") ) {
                endpoint = endpoint.substring(0,endpoint.length()-1);
            }
            return endpoint;
        }
    }
    
    @Override 
    public @Nonnull RackspaceNetworkServices getNetworkServices() {
        return new RackspaceNetworkServices(this);
    }
    
    @Override
    public @Nonnull RackspacePlatformServices getPlatformServices() {
        return new RackspacePlatformServices(this);
    }
    
    @Override
    public @Nonnull String getProviderName() {
        return "Rackspace";
    }
    
    public boolean isMyRegion() throws CloudException, InternalException {
        ProviderContext ctx = getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context has been set.");
        }
        boolean uk = isUK();
        
        if( uk && "LON".equals(ctx.getRegionId()) ) {
            return true;
        }
        else if( !uk ) {
            String region = ctx.getRegionId();
            
            return (region == null || getAuthenticationContext().getMyRegion().equals(region));
        }
        return false;
    }
    
    public boolean isUK() {
        return (getEndpoint().startsWith("https://lon") || getEndpoint().startsWith("http://lon")); 
    }
    
    @Override
    public @Nonnull RackspaceStorageServices getStorageServices() {
        return new RackspaceStorageServices(this);
    }
    
    public @Nonnegative long parseTimestamp(String time) throws CloudException {
        if( time == null ) {
            return 0L;
        }
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            
        if( time.length() > 0 ) {
            try {
                return fmt.parse(time).getTime();
            } 
            catch( ParseException e ) {
                fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                try {
                    return fmt.parse(time).getTime();
                } 
                catch( ParseException encore ) {
                    throw new CloudException("Could not parse date: " + time);
                }
            }
        }
        return 0L;
    }
    
    @Override
    public @Nullable String testContext() {
        Logger logger = getLogger(RackspaceCloud.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + RackspaceCloud.class.getName() + ".textContext()");
        }
        try {
            ProviderContext ctx = getContext();

            if( ctx == null ) {
                return null;
            }
            try {
                String pk = new String(ctx.getAccessPublic(), "utf-8");
                RackspaceMethod method = new RackspaceMethod(this);
                
                if( method.authenticate() != null ) {
                    return pk;
                }
                return null;
            }
            catch( Throwable t ) {
                logger.warn("Failed to test Rackspace connection context: " + t.getMessage());
                if( logger.isTraceEnabled() ) {
                    t.printStackTrace();
                }
                return null;
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + RackspaceCloud.class.getName() + ".testContext()");
            }
        }
    }
}
