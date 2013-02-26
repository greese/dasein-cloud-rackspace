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

public class AuthenticationContext {    
    private String         authToken;
    private String         myRegion;
    private String         storageToken;
    private String         cdnUrl;
    private String         serverUrl;
    private String         storageUrl;
    
    public AuthenticationContext() { }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public String getStorageToken() {
        if( storageToken == null ) {
            return getAuthToken();
        }
        return storageToken;
    }

    public void setStorageToken(String storageToken) {
        this.storageToken = storageToken;
    }

    public String getCdnUrl() {
        return cdnUrl;
    }

    public void setCdnUrl(String cdnUrl) {
        this.cdnUrl = cdnUrl;
    }

    public String getLoadBalancerUrl(String regionId) {
        if( serverUrl == null ) {
            return null;
        }
        regionId = regionId.toLowerCase();
        if( regionId.equals("lon") ) {
            return serverUrl.replaceAll("servers", "loadbalancers");            
        }
        else {
            return serverUrl.replaceAll("servers", regionId.toLowerCase() + ".loadbalancers");
        }
    }
    
    public String getMyRegion() {
        return myRegion;
    }
    
    public String getServerUrl() {
        return serverUrl;
    }

    public void setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String getStorageUrl() {
        return storageUrl;
    }

    public void setStorageUrl(String storageUrl) {
        if( storageUrl != null ) {
            String tmp = storageUrl.toLowerCase();
            
            if( tmp.contains(".dfw") ) {
                myRegion = "xDFW";
            }
            else if( tmp.contains(".ord") ) {
                myRegion = "xORD";
            }
            else if( tmp.contains(".lon") ) {
                myRegion = "xLON";
            }
            else {
                myRegion = "xORD";
            }
        }
        this.storageUrl = storageUrl;
    }
    
}
