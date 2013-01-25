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

package org.dasein.cloud.rackspace.platform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.platform.CDNSupport;
import org.dasein.cloud.platform.Distribution;
import org.dasein.cloud.rackspace.CDNMethod;
import org.dasein.cloud.rackspace.RackspaceCloud;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;

public class CloudCDN implements CDNSupport {
    private RackspaceCloud provider;
    
    public CloudCDN(RackspaceCloud provider) { this.provider = provider; }
    
    @Override
    public String create(String origin, String name, boolean active, String... aliases) throws InternalException, CloudException {
        CDNMethod method = new CDNMethod(provider);
        
        method.put(origin);
        return origin;
    }

    @Override
    public void delete(String distributionId) throws InternalException, CloudException {
        CDNMethod method = new CDNMethod(provider);
        
        method.post(distributionId, false);        
    }

    @Override
    public Distribution getDistribution(String distributionId) throws InternalException, CloudException {
        CDNMethod method = new CDNMethod(provider);
        Map<String,String> metaData = method.head(distributionId);
        
        if( metaData == null ) {
            return null;
        }
        Distribution distribution = new Distribution();
            
        distribution.setActive(true);
        distribution.setAliases(new String[0]);
        
        String enabled = metaData.get("X-CDN-Enabled");
        
        distribution.setDeployed(enabled == null ? false : enabled.toLowerCase().equals("true"));
        
        String dnsName = metaData.get("X-CDN-SSL-URI");
        String prefix = "http://";
        
        if( dnsName == null ) {
            dnsName = metaData.get("X-CDN-URI");
            if( dnsName == null ) {
                return null;
            }
            if( dnsName.startsWith("http://") ) {
                dnsName = dnsName.substring("http://".length());
            }
        }
        else {
            if( dnsName.startsWith("https://") ) {
                dnsName = dnsName.substring("https://".length());
                prefix = "https://";
            }
        }
        distribution.setDnsName(dnsName);
        distribution.setLocation(prefix + distribution.getDnsName() + "/" + distributionId);
        distribution.setLogDirectory(null);
        distribution.setLogName(null);
        distribution.setName(distributionId);
        distribution.setProviderDistributionId(distributionId);
        distribution.setProviderOwnerId(provider.getContext().getAccountNumber());
        return distribution;
    }

    @Override
    public String getProviderTermForDistribution(Locale locale) {
        return "distribution";
    }

    @Override
    public boolean isSubscribed() throws InternalException, CloudException {
        return (provider.testContext() != null);
    }

    @Override
    public Collection<Distribution> list() throws InternalException, CloudException {
        ArrayList<Distribution> distributions = new ArrayList<Distribution>();
        CDNMethod method = new CDNMethod(provider);
        JSONArray list = method.get();
        
        if( list != null ) {
            for( int i=0; i<list.length(); i++ ) {
                try {
                    JSONObject ob = list.getJSONObject(i);
                    Distribution d = new Distribution();
                    String name = ob.getString("name");
                    
                    d.setActive(true);
                    d.setAliases(new String[0]);
                    
                    if( ob.has("cdn_enabled") ) {
                        String enabled = ob.getString("cdn_enabled");
                    
                        d.setDeployed(enabled == null ? false : enabled.toLowerCase().equals("true"));
                    }
                    else {
                        d.setDeployed(false);
                    }
                    String dnsName = null;
                    String prefix = "http://";
                    
                    if( ob.has("cdn_ssl_uri") ) {
                        dnsName = ob.getString("cdn_ssl_uri");
                        if( dnsName != null && dnsName.startsWith("https://") ) {
                            dnsName = dnsName.substring("https://".length());
                            prefix = "https://";
                        }
                    }
                    if( dnsName == null && ob.has("cdn_uri") ) {
                        dnsName = ob.getString("cdn_uri");
                        if( dnsName != null && dnsName.startsWith("http://") ) {
                            dnsName = dnsName.substring("http://".length());
                        }
                    }
                    d.setDnsName(dnsName);
                    d.setLocation(prefix + d.getDnsName() + "/" + name);
                    d.setLogDirectory(null);
                    d.setLogName(null);
                    d.setName(name);
                    d.setProviderDistributionId(name);
                    d.setProviderOwnerId(provider.getContext().getAccountNumber());
                    
                    distributions.add(d);
                }
                catch( JSONException e ) {
                    throw new CloudException("JSON error in distribution: " + e.getMessage());
                }
            }
        }
        return distributions;
    }

    @Nonnull
    @Override
    public Iterable<ResourceStatus> listDistributionStatus() throws InternalException, CloudException {
        ArrayList<ResourceStatus> status = new ArrayList<ResourceStatus>();

        for( Distribution d : list() ) {
            status.add(new ResourceStatus(d.getProviderDistributionId(), d.isDeployed()));
        }
        return status;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void update(String distributionId, String name, boolean active, String... aliases) throws InternalException, CloudException {
        CDNMethod method = new CDNMethod(provider);
        
        method.post(distributionId, active); 
    }
}
