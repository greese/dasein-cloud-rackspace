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

import java.util.HashMap;
import java.util.Map;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.json.JSONArray;
import org.json.JSONException;

public class CDNMethod extends AbstractMethod {
    public CDNMethod(RackspaceCloud provider) { super(provider); }

    public JSONArray get() throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        
        String response = getString(context.getStorageToken(), context.getCdnUrl(), "?format=json");
        
        if( response == null ) {
            return null;
        }
        try {
            return new JSONArray(response);
        }
        catch( JSONException e ) {
            throw new CloudException("Invalid JSON from server: " + e.getMessage());
        }
    }
    
    public Map<String,String> head(String container) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        
        return head(context.getStorageToken(), context.getCdnUrl(), "/" + container);        
    }
    
    public void post(String container, boolean enabled) throws CloudException, InternalException {
        HashMap<String,String> customHeaders = new HashMap<String,String>();
        
        customHeaders.put("X-Log-Retention", "True");
        customHeaders.put("X-CDN-Enabled", (enabled ? "True" : "False"));
        AuthenticationContext context = provider.getAuthenticationContext();
       
        putHeaders(context.getStorageToken(), context.getCdnUrl(), "/" + container, customHeaders);
    }
    
    public void put(String container) throws CloudException, InternalException {
        HashMap<String,String> customHeaders = new HashMap<String,String>();
        
        customHeaders.put("X-Log-Retention", "True");
        customHeaders.put("X-CDN-Enabled", "True");

        AuthenticationContext context = provider.getAuthenticationContext();
       
        putHeaders(context.getStorageToken(), context.getCdnUrl(), "/" + container, customHeaders);
    }
}
