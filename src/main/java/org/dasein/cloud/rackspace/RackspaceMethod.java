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

import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.json.JSONException;
import org.json.JSONObject;

public class RackspaceMethod extends AbstractMethod {
    public RackspaceMethod(RackspaceCloud provider) { super(provider); }
   
    public void deleteLoadBalancers(String resource, String resourceId) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context has been set for this request");
        }
        delete(context.getAuthToken(), context.getLoadBalancerUrl(ctx.getRegionId()), resource + "/" + resourceId);
    }
    
    public void deleteServers(String resource, String resourceId) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();

        delete(context.getAuthToken(), context.getServerUrl(), resource + "/" + resourceId);
    }
    
    public JSONObject getLoadBalancers(String resource, String resourceId) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();

        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context has been set for this request");
        }
        if( resourceId != null ) {
            resource = resource + "/" + resourceId;
        }
        String response = getString(context.getAuthToken(), context.getLoadBalancerUrl(ctx.getRegionId()), resource);

        if( response == null ) {
            return null;
        }
        try {
            return new JSONObject(response);
        }
        catch( JSONException e ) {
            throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", response);
        }
    }
    
    public JSONObject getServers(String resource, String resourceId) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        
        if( resourceId != null ) {
            resource = resource + "/" + resourceId;
        }
        else {
            resource = resource + "/detail";
        }
        String response = getString(context.getAuthToken(), context.getServerUrl(), resource);

        if( response == null ) {
            return null;
        }
        try {
            return new JSONObject(response);
        }
        catch( JSONException e ) {
            throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", response);
        }
    }
    

    
    public JSONObject postLoadBalancers(String resource, String resourceId, JSONObject body) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context has been set for this request");
        }
        if( resourceId != null ) {
            resource = resource + "/" + resourceId;
        }
        String response = postString(context.getAuthToken(), context.getLoadBalancerUrl(ctx.getRegionId()), resource, body.toString());
        
        if( response == null ) {
            return null;
        }
        try {
            return new JSONObject(response);
        }
        catch( JSONException e ) {
            throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", response);
        }
    }
    
    public JSONObject postServers(String resource, String resourceId, JSONObject body) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        
        if( resourceId != null ) {
            resource = resource + "/" + resourceId + "/action";
        }
        String response = postString(context.getAuthToken(), context.getServerUrl(), resource, body.toString());
        
        if( response == null ) {
            return null;
        }
        try {
            return new JSONObject(response);
        }
        catch( JSONException e ) {
            throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", response);
        }
    }
}

