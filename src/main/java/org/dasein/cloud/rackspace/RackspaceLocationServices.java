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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Region;

public class RackspaceLocationServices implements DataCenterServices {
    private RackspaceCloud provider;
    
    public RackspaceLocationServices(RackspaceCloud provider) { this.provider = provider; }
    
    @Override
    public DataCenter getDataCenter(String providerDataCenterId) throws InternalException, CloudException {
        Logger std = RackspaceCloud.getLogger(RackspaceLocationServices.class, "std");
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + RackspaceLocationServices.class.getName() + ".getDataCenter(" + providerDataCenterId + ")");
        }
        try {
            if( providerDataCenterId.charAt(providerDataCenterId.length()-1) != '1' ) {
                std.warn("getDataCenter(): Invalid data center ID: " + providerDataCenterId);
                return null;
            }
            
            String providerRegionId = providerDataCenterId.substring(0, providerDataCenterId.length()-1);
            Region region = getRegion(providerRegionId);
            
            if( region == null ) {
                std.warn("getDataCenter(): Invalid region " + providerRegionId + " for desired data center");
                return null;
            }
            DataCenter dc = new DataCenter();
            
            dc.setActive(true);
            dc.setAvailable(true);
            dc.setName(region.getName() + " (1)");
            dc.setProviderDataCenterId(providerRegionId + "1");
            dc.setRegionId(providerRegionId);
            if( std.isTraceEnabled() ) {
                std.trace("getDataCenter(): " + providerDataCenterId + "=" + dc);
            }
            return dc;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + RackspaceLocationServices.class.getName() + ".getDataCenter()");
            }            
        }
    }

    @Override
    public String getProviderTermForDataCenter(Locale locale) {
        return "data center";
    }

    @Override
    public String getProviderTermForRegion(Locale locale) {
        return "region";
    }

    @Override
    public Region getRegion(String providerRegionId) throws InternalException, CloudException {
        Logger std = RackspaceCloud.getLogger(RackspaceLocationServices.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + RackspaceLocationServices.class.getName() + ".getRegion(" + providerRegionId + ")");
        }
        try {
            Region region = null;
            
            if( provider.isUK() ) {
                if( providerRegionId.equals("xLON") ) {
                    region = new Region();                
                    region.setActive(true);
                    region.setAvailable(true);
                    region.setJurisdiction("EU");
                    region.setName("London/Legacy");
                    region.setProviderRegionId(providerRegionId);
                }
            }
            else {
                if( providerRegionId.equals("xORD") ) {
                    region = new Region();                
                    region.setActive(true);
                    region.setAvailable(true);
                    region.setJurisdiction("US");
                    region.setName("Chicago/Legacy");
                    region.setProviderRegionId(providerRegionId);
                }
                else if( providerRegionId.equals("xDFW") ) {
                    region = new Region();                
                    region.setActive(true);
                    region.setAvailable(true);
                    region.setJurisdiction("US");
                    region.setName("Dallas/Legacy");
                    region.setProviderRegionId(providerRegionId);
                }
            }
            return region;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + RackspaceLocationServices.class.getName() + ".getRegion()");
            }            
        }
    }

    @Override
    public Collection<DataCenter> listDataCenters(String providerRegionId) throws InternalException, CloudException {
        Logger std = RackspaceCloud.getLogger(RackspaceLocationServices.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + RackspaceLocationServices.class.getName() + ".listDataCenters(" + providerRegionId + ")");
        }
        try {
            DataCenter dc = getDataCenter(providerRegionId + "1");
            
            if( dc == null ) {
                return Collections.emptyList();
            }
            return Collections.singletonList(dc);
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + RackspaceLocationServices.class.getName() + ".listDataCenters()");
            }            
        }
    }

    @Override
    public Collection<Region> listRegions() throws InternalException, CloudException {
        Logger std = RackspaceCloud.getLogger(RackspaceLocationServices.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + RackspaceLocationServices.class.getName() + ".listRegions()");
        }
        try {
            if( provider.isUK() ) {
                return Collections.singletonList(getRegion("xLON"));
            }
            else {
                ArrayList<Region> regions = new ArrayList<Region>();
                
                regions.add(getRegion("xORD"));
                regions.add(getRegion("xDFW"));
                return regions;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + RackspaceLocationServices.class.getName() + ".listRegions()");
            }            
        }
    }

}
