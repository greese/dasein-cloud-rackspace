package org.dasein.cloud.rackspace;

import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.compute.ComputeServices;
import org.dasein.cloud.dc.Region;
import org.dasein.cloud.network.NetworkServices;
import org.dasein.cloud.openstack.nova.os.NovaOpenStack;
import org.dasein.cloud.platform.PlatformServices;
import org.dasein.cloud.storage.StorageServices;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

/**
 * [Class Documentation]
 * <p>Created by George Reese: 2/25/13 4:56 PM</p>
 *
 * @author George Reese
 */
public class UnifiedCloud extends AbstractCloud {
    private boolean        connected;
    private RackspaceCloud legacy;
    private NovaOpenStack  openstack;

    private synchronized void connect() {
        ProviderContext ctx = getContext();

        if( ctx != null ) {
            String endpoint = ctx.getEndpoint();

            if( endpoint != null ) {
                String[] parts = endpoint.split(";");

                if( parts.length < 1 ) {
                    if( endpoint.endsWith("v1.0") || endpoint.endsWith("v1.0/") ) {
                        connectLegacy(ctx, endpoint);
                    }
                    else {
                        connectOpenstack(ctx, endpoint);
                    }
                }
                else {
                    if( parts[0].endsWith("v1.0") || parts[0].endsWith("v1.0/") ) {
                        connectLegacy(ctx, parts[0]);
                    }
                    else {
                        connectOpenstack(ctx, parts[0]);
                    }
                    if( parts.length > 1 ) {
                        if( parts[1].endsWith("v1.0") || parts[1].endsWith("v1.0/") ) {
                            connectLegacy(ctx, parts[1]);
                        }
                        else {
                            connectOpenstack(ctx, parts[1]);
                        }
                    }
                }
                connected = true;
            }
        }
    }

    private void connectLegacy(@Nonnull ProviderContext ctx, @Nonnull String endpoint) {
        ProviderContext copy = new ProviderContext();

        legacy = new RackspaceCloud();

        copy.setAccountNumber(ctx.getAccountNumber());
        copy.setAccessPublic(ctx.getAccessPublic());
        copy.setAccessPrivate(ctx.getAccessPrivate());
        copy.setEndpoint(endpoint);
        copy.setRegionId(ctx.getRegionId());
        Properties p = ctx.getCustomProperties();

        if( p != null ) {
            copy.setCustomProperties(p);
        }
        copy.setCloudName("The Rackspace Cloud");
        copy.setProviderName("Rackspace");
        legacy.connect(copy);
    }

    private void connectOpenstack(@Nonnull ProviderContext ctx, @Nonnull String endpoint) {
        ProviderContext copy = new ProviderContext();

        openstack = new NovaOpenStack();

        copy.setAccountNumber(ctx.getAccountNumber());
        copy.setAccessPublic(ctx.getAccessPublic());
        copy.setAccessPrivate(ctx.getAccessPrivate());
        copy.setEndpoint(endpoint);
        copy.setRegionId(ctx.getRegionId());
        Properties p = ctx.getCustomProperties();

        if( p != null ) {
            copy.setCustomProperties(p);
        }
        copy.setCloudName("The Rackspace Cloud");
        copy.setProviderName("Rackspace");
        openstack.connect(copy);
    }

    @Override
    public @Nonnull String getCloudName() {
        return "The Rackspace Cloud";
    }

    @Override
    public @Nullable ComputeServices getComputeServices() {
        synchronized( this ) {
            if( !connected ) {
                connect();
            }
        }
        try {
            ProviderContext ctx = getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            String regionId = ctx.getRegionId();

            if( regionId != null && legacy != null && isLegacy(regionId) ) {
                return legacy.getComputeServices();
            }
            if( openstack != null ) {
                return openstack.getComputeServices();
            }
            return null;
        }
        catch( CloudException e ) {
            throw new RuntimeException("Unable to load unified compute services: " + e.getMessage());
        }
        catch( InternalException e ) {
            throw new RuntimeException("Unable to load unified compute services: " + e.getMessage());
        }
    }

    @Override
    public @Nonnull UnifiedDC getDataCenterServices() {
        synchronized( this ) {
            if( !connected ) {
                connect();
            }
        }
        return new UnifiedDC(this);
    }

    public @Nullable RackspaceCloud getLegacyCloud() {
        return legacy;
    }

    @Override
    public @Nullable NetworkServices getNetworkServices() {
        synchronized( this ) {
            if( !connected ) {
                connect();
            }
        }
        try {
            ProviderContext ctx = getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            String regionId = ctx.getRegionId();

            if( regionId != null && legacy != null && isLegacy(regionId) ) {
                return legacy.getNetworkServices();
            }
            if( openstack != null ) {
                return openstack.getNetworkServices();
            }
            return null;
        }
        catch( CloudException e ) {
            throw new RuntimeException("Unable to load unified compute services: " + e.getMessage());
        }
        catch( InternalException e ) {
            throw new RuntimeException("Unable to load unified compute services: " + e.getMessage());
        }
    }

    public @Nullable NovaOpenStack getOpenstackCloud() {
        return openstack;
    }

    @Override
    public @Nullable PlatformServices getPlatformServices() {
        synchronized( this ) {
            if( !connected ) {
                connect();
            }
        }
        try {
            ProviderContext ctx = getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            String regionId = ctx.getRegionId();

            if( regionId != null && legacy != null && isLegacy(regionId) ) {
                return legacy.getPlatformServices();
            }
            if( openstack != null ) {
                return openstack.getPlatformServices();
            }
            return null;
        }
        catch( CloudException e ) {
            throw new RuntimeException("Unable to load unified compute services: " + e.getMessage());
        }
        catch( InternalException e ) {
            throw new RuntimeException("Unable to load unified compute services: " + e.getMessage());
        }
    }

    @Override
    public @Nonnull String getProviderName() {
        return "Rackspace";
    }

    @Override
    public @Nullable StorageServices getStorageServices() {
        synchronized( this ) {
            if( !connected ) {
                connect();
            }
        }
        try {
            ProviderContext ctx = getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            String regionId = ctx.getRegionId();

            if( regionId != null && legacy != null && isLegacy(regionId) ) {
                return legacy.getStorageServices();
            }
            if( openstack != null ) {
                return openstack.getStorageServices();
            }
            return null;
        }
        catch( CloudException e ) {
            throw new RuntimeException("Unable to load unified compute services: " + e.getMessage());
        }
        catch( InternalException e ) {
            throw new RuntimeException("Unable to load unified compute services: " + e.getMessage());
        }
    }

    public boolean isLegacy(@Nonnull String regionId) throws CloudException, InternalException {
        ProviderContext ctx = getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        Cache<String> cache = Cache.getInstance(this, "legacyCache", String.class, CacheLevel.CLOUD_ACCOUNT, new TimePeriod<Day>(1, TimePeriod.DAY));
        Iterable<String> legacies = cache.get(ctx);

        if( legacies == null ) {
            if( legacy == null ) {
                legacies = Collections.emptyList();
            }
            else {
                ArrayList<String> tmp = new ArrayList<String>();

                for( Region r : legacy.getDataCenterServices().listRegions() ) {
                    tmp.add(r.getProviderRegionId());
                }
                legacies = tmp;
            }
            cache.put(ctx, legacies);
        }
        for( String s : legacies ) {
            if( s.equals(regionId) ) {
                return true;
            }
        }
        return false;
    }

    @Override
    public @Nullable String testContext() {
        try {
            synchronized( this ) {
                if( !connected ) {
                    connect();
                }
                if( openstack != null ) {
                    return openstack.testContext();
                }
                if( legacy != null ) {
                    return legacy.testContext();
                }
                return null;
            }
        }
        catch( Throwable t ) {
            return null;
        }
    }
}
