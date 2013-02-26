package org.dasein.cloud.rackspace;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Region;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

/**
 * [Class Documentation]
 * <p>Created by George Reese: 2/25/13 4:57 PM</p>
 *
 * @author George Reese
 */
public class UnifiedDC implements DataCenterServices {
    private UnifiedCloud provider;

    public UnifiedDC(@Nonnull UnifiedCloud cloud) {
        this.provider = cloud;
    }

    @Override
    public DataCenter getDataCenter(String providerDataCenterId) throws InternalException, CloudException {
        for( Region r : listRegions() ) {
            for( DataCenter dc : listDataCenters(r.getProviderRegionId()) ) {
                if( providerDataCenterId.equals(dc.getProviderDataCenterId()) ) {
                    return dc;
                }
            }
        }
        return null;
    }

    @Override
    public String getProviderTermForDataCenter(Locale locale) {
        return "zone";
    }

    @Override
    public String getProviderTermForRegion(Locale locale) {
        return "region";
    }

    @Override
    public Region getRegion(String providerRegionId) throws InternalException, CloudException {
        NovaOpenStack openstack = provider.getOpenstackCloud();

        if( openstack != null ) {
            Region r = openstack.getDataCenterServices().getRegion(providerRegionId);

            if( r != null ) {
                return r;
            }
        }
        RackspaceCloud legacy = provider.getLegacyCloud();

        if( legacy != null ) {
            Region r = legacy.getDataCenterServices().getRegion(providerRegionId);

            if( r != null ) {
                return r;
            }
        }
        return null;
    }

    @Override
    public Collection<DataCenter> listDataCenters(String providerRegionId) throws InternalException, CloudException {
        NovaOpenStack openstack = provider.getOpenstackCloud();

        if( openstack != null ) {
            Region r = openstack.getDataCenterServices().getRegion(providerRegionId);

            if( r != null ) {
                return openstack.getDataCenterServices().listDataCenters(providerRegionId);
            }
        }
        RackspaceCloud legacy = provider.getLegacyCloud();

        if( legacy != null ) {
            Region r = legacy.getDataCenterServices().getRegion(providerRegionId);

            if( r != null ) {
                return legacy.getDataCenterServices().listDataCenters(providerRegionId);
            }
        }
        return null;
    }

    @Override
    public Collection<Region> listRegions() throws InternalException, CloudException {
        ArrayList<Region> regions = new ArrayList<Region>();

        NovaOpenStack openstack = provider.getOpenstackCloud();

        if( openstack != null ) {
            regions.addAll(openstack.getDataCenterServices().listRegions());
        }

        RackspaceCloud legacy = provider.getLegacyCloud();

        if( legacy != null ) {
            regions.addAll(legacy.getDataCenterServices().listRegions());
        }
        return regions;
    }
}
