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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;

public class CloudFilesMethod extends AbstractMethod {
    public CloudFilesMethod(RackspaceCloud provider) { super(provider); }
        
    public void delete(String bucket) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        
        delete(context.getStorageToken(), context.getStorageUrl(), "/" + bucket);
    }
    
    public void delete(String bucket, String object) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        
        delete(context.getStorageToken(), context.getStorageUrl(), "/" + bucket + "/" + object);
    }
    
    public List<String> get(String bucket) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        
        String response = getString(context.getStorageToken(), context.getStorageUrl(), bucket == null ? "/" : "/" + bucket);
        ArrayList<String> entries = new ArrayList<String>();

        if( response != null ) {
            response = response.trim();
            if( response.length() > 0 ) {
                String[] lines = response.split("\n");
                
                if( lines.length < 1 ) {
                    entries.add(response);
                }
                else {
                    for( String line : lines ) {
                        entries.add(line.trim());
                    }
                }

            }
        }        
        return entries;
    }

    public InputStream get(String bucket, String object) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        
        return getStream(context.getStorageToken(), context.getStorageUrl(), "/" + bucket + "/" + object);
    }

    @SuppressWarnings("unused")
    public Map<String,String> head(String bucket) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        
        return head(context.getStorageToken(), context.getStorageUrl(), "/" + bucket);        
    }
    
    public Map<String,String> head(String bucket, String object) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        
        return head(context.getStorageToken(), context.getStorageUrl(), "/" + bucket + "/" + object);        
    }
    
    public void put(String bucket) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        
        putString(context.getStorageToken(), context.getStorageUrl(), "/" + bucket, null);
    }
    
    public void put(String bucket, String object, String md5Hash, InputStream payload) throws CloudException, InternalException {
        AuthenticationContext context = provider.getAuthenticationContext();
        
        putStream(context.getStorageToken(), context.getStorageUrl(), "/" + bucket + "/" + object, md5Hash, payload);
    }
}
