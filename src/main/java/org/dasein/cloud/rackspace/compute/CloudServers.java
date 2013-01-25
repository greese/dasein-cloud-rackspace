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

package org.dasein.cloud.rackspace.compute;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VMLaunchOptions;
import org.dasein.cloud.compute.VMScalingCapabilities;
import org.dasein.cloud.compute.VMScalingOptions;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VmStatistics;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.rackspace.RackspaceCloud;
import org.dasein.cloud.rackspace.RackspaceException;
import org.dasein.cloud.rackspace.RackspaceMethod;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class CloudServers implements VirtualMachineSupport {
    static private HashMap<String,Collection<VirtualMachineProduct>> cachedProducts = new HashMap<String,Collection<VirtualMachineProduct>>();
    
    static public final int NAME_LIMIT = 30;
    static public final int TAG_LIMIT  = 4;
    
    private RackspaceCloud provider;
    
    CloudServers(@Nonnull RackspaceCloud provider) { this.provider = provider; }

    @Override
    public VirtualMachine alterVirtualMachine(@Nonnull String vmId, @Nonnull VMScalingOptions options) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Not supported");
    }

    @Override
    public @Nonnull VirtualMachine clone(@Nonnull String vmId, @Nullable String intoDcId, @Nonnull String name, @Nonnull String description, boolean powerOn, @Nullable String... firewallIds) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Rackspace foes not support the cloning of servers.");
    }

    @Override
    public VMScalingCapabilities describeVerticalScalingCapabilities() throws CloudException, InternalException {
        return null;
    }

    @Override
    public void disableAnalytics(@Nonnull String vmId) throws InternalException, CloudException {
        // NO-OP
    }

    @Override
    public void enableAnalytics(@Nonnull String vmId) throws InternalException, CloudException {
        // NO-OP
    }

    @Override
    public @Nonnull String getConsoleOutput(@Nonnull String vmId) throws InternalException, CloudException {
        return "";
    }

    @Override
    public int getCostFactor(@Nonnull VmState state) throws InternalException, CloudException {
        return 100;
    }

    @Override
    public int getMaximumVirtualMachineCount() throws CloudException, InternalException {
        return -2;
    }

    @Override
    public @Nullable VirtualMachineProduct getProduct(@Nonnull String productId) throws InternalException, CloudException {
        Logger std = RackspaceCloud.getLogger(CloudServers.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + CloudServers.class.getName() + ".getProduct(" + productId + ")");
        }
        try {
            if( !provider.isMyRegion() ) {
                return null;
            }
            for( VirtualMachineProduct product : listProducts(Architecture.I64) ) {
                if( product.getProviderProductId().equals(productId) ) {
                    return product;
                }
            }
            return null;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + CloudServers.class.getName() + ".getProduct()");
            }
        }
    }

    @Override
    public @Nonnull String getProviderTermForServer(@Nonnull Locale locale) {
        return "server";
    }

    @Override
    public @Nullable VirtualMachine getVirtualMachine(@Nonnull String vmId) throws InternalException, CloudException {
        Logger std = RackspaceCloud.getLogger(CloudServers.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + CloudServers.class.getName() + ".getVirtualMachine(" + vmId + ")");
        }
        try {
            /*
            if( !provider.isMyRegion() ) {
                return null;
            }
            */
            RackspaceMethod method = new RackspaceMethod(provider);
            JSONObject ob = method.getServers("/servers", vmId);

            if( ob == null ) {
                return null;
            }
            try {
                if( ob.has("server") ) {
                    JSONObject server = ob.getJSONObject("server");
                    VirtualMachine vm = toVirtualMachine(server);
                        
                    if( vm != null ) {
                        return vm;
                    }
                }
            }
            catch( JSONException e ) {
                std.error("getVirtualMachine(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for servers");
            }
            return null;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + CloudServers.class.getName() + ".getVirtualMachine()");
            }
        }
    }

    @Override
    public @Nullable VmStatistics getVMStatistics(@Nonnull String vmId, long from, long to) throws InternalException, CloudException {
        return null;
    }

    @Override
    public @Nonnull Iterable<VmStatistics> getVMStatisticsForPeriod(@Nonnull String vmId, long from, long to) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Requirement identifyImageRequirement(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return (cls.equals(ImageClass.MACHINE) ? Requirement.REQUIRED : Requirement.NONE);
    }

    @Override
    public @Nonnull Requirement identifyPasswordRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Nonnull
    @Override
    public Requirement identifyPasswordRequirement(Platform platform) throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyRootVolumeRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyShellKeyRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Nonnull
    @Override
    public Requirement identifyShellKeyRequirement(Platform platform) throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Nonnull
    @Override
    public Requirement identifyStaticIPRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyVlanRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public boolean isAPITerminationPreventable() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isBasicAnalyticsSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isExtendedAnalyticsSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return (provider.isMyRegion() && provider.testContext() != null);
    }

    @Override
    public boolean isUserDataSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull VMLaunchOptions withLaunchOptions) throws CloudException, InternalException {
        Logger logger = RackspaceCloud.getLogger(CloudServers.class, "std");

        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + CloudServers.class.getName() + ".launch(" + withLaunchOptions + ")");
        }
        try {
            if( !provider.isMyRegion() ) {
                throw new CloudException("Unable to launch any servers in " + provider.getContext().getRegionId());
            }
            String fromMachineImageId = withLaunchOptions.getMachineImageId();
            String productId = withLaunchOptions.getStandardProductId();
            Map<String,Object> meta = withLaunchOptions.getMetaData();

            MachineImage targetImage = provider.getComputeServices().getImageSupport().getImage(fromMachineImageId);
            HashMap<String,Object> wrapper = new HashMap<String,Object>();
            HashMap<String,Object> json = new HashMap<String,Object>();

            json.put("imageId", Long.parseLong(fromMachineImageId));
            json.put("flavorId", Long.parseLong(productId));
            HashMap<String,Object> metaData = new HashMap<String,Object>();
            boolean safeName = false;
            int tagCount = 0;

            if( meta != null ) {
                for( Map.Entry<String,Object> tag : meta.entrySet() ) {
                    if( tag.getKey() != null && tag.getValue() != null && tag.getKey().length() > 0 && tag.getValue().toString().length() > 0 ) {
                        metaData.put(tag.getKey(), tag.getValue());
                        tagCount++;
                        if( tagCount >= TAG_LIMIT ) {
                            break;
                        }
                    }
                }
            }
            if( tagCount < TAG_LIMIT && !targetImage.getPlatform().equals(Platform.UNKNOWN) ) {
                metaData.put("dsnPlatform", targetImage.getPlatform().name());
                tagCount++;
            }
            if( tagCount < TAG_LIMIT ) {
                metaData.put("dsnDescription", withLaunchOptions.getDescription());
                tagCount++;
            }
            if( tagCount < TAG_LIMIT ) {
                metaData.put("dsnName", withLaunchOptions.getFriendlyName());
                safeName = true;
            }
            metaData.put("dsnTrueImage", targetImage.getProviderMachineImageId());
            json.put("metadata", metaData);
            json.put("name", validateName(withLaunchOptions.getHostName(), safeName));

            wrapper.put("server", json);
            RackspaceMethod method = new RackspaceMethod(provider);
            JSONObject result = method.postServers("/servers", null, new JSONObject(wrapper));

            if( result.has("server") ) {
                try {
                    JSONObject server = result.getJSONObject("server");
                    VirtualMachine vm = toVirtualMachine(server);

                    if( vm != null ) {
                        return vm;
                    }
                }
                catch( JSONException e ) {
                    logger.error("launch(): Unable to understand launch response: " + e.getMessage());
                    if( logger.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);
                }
            }
            logger.error("launch(): No server was created by the launch attempt, and no error was returned");
            throw new CloudException("No virtual machine was launched");

        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + CloudServers.class.getName() + ".launch()");
            }
        }
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull String fromMachineImageId, @Nonnull VirtualMachineProduct product, @Nullable String dataCenterId, @Nonnull String name, @Nonnull String description, @Nullable String withKeypairId, @Nullable String inVlanId, boolean withAnalytics, boolean asSandbox, @Nullable String... firewallIds) throws InternalException, CloudException {
        return launch(fromMachineImageId, product, dataCenterId, name, description, withKeypairId, inVlanId, withAnalytics, asSandbox, firewallIds, new Tag[0]);
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull String fromMachineImageId, @Nonnull VirtualMachineProduct product, @Nullable String dataCenterId, @Nonnull String name, @Nonnull String description, @Nullable String withKeypairId, @Nullable String inVlanId, boolean withAnalytics, boolean asSandbox, @Nullable String[] firewallIds, @Nullable Tag... tags) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was provided for this request");
        }
        //noinspection ConstantConditions
        if( description == null ) {
            description = name;
        }
        if( dataCenterId == null ) {
            for( DataCenter dc : provider.getDataCenterServices().listDataCenters(ctx.getRegionId()) ) {
                if( dc != null ) {
                    dataCenterId = dc.getProviderDataCenterId();
                    break;
                }
            }
        }
        VMLaunchOptions options;

        if( inVlanId == null ) {
            //noinspection ConstantConditions
            options = VMLaunchOptions.getInstance(product.getProviderProductId(), fromMachineImageId, name, description).inDataCenter(dataCenterId);
        }
        else {
            //noinspection ConstantConditions
            options = VMLaunchOptions.getInstance(product.getProviderProductId(), fromMachineImageId, name, description).inVlan(null, dataCenterId, inVlanId);
        }
        if( withKeypairId != null ) {
            options = options.withBoostrapKey(withKeypairId);
        }
        if( tags != null ) {
            for( Tag t : tags ) {
                options = options.withMetaData(t.getKey(), t.getValue());
            }
        }
        if( firewallIds != null ) {
            options = options.behindFirewalls(firewallIds);
        }
        return launch(options);
    }

    @Override
    public @Nonnull Iterable<String> listFirewalls(@Nonnull String vmId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<VirtualMachineProduct> listProducts(@Nonnull Architecture architecture) throws InternalException, CloudException {
       Logger std = RackspaceCloud.getLogger(CloudServers.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + CloudServers.class.getName() + ".listProducts()");
        }
        try {
            if( !provider.isMyRegion() ) {
                return Collections.emptyList();
            }
            if( architecture.equals(Architecture.I32) ) {
                return Collections.emptyList();
            }
            Collection<VirtualMachineProduct> products = cachedProducts.get(provider.getContext().getRegionId());
            
            if( products == null ) {
                if( std.isDebugEnabled() ) {
                    std.debug("listProducts(): Cache for " + provider.getContext().getRegionId() + " is empty, fetching values from cloud");
                }
                RackspaceMethod method = new RackspaceMethod(provider);
                JSONObject ob = method.getServers("/flavors", null);
                
                products = new ArrayList<VirtualMachineProduct>();
                try {
                    if( ob.has("flavors") ) {
                        JSONArray list = ob.getJSONArray("flavors");
                        
                        for( int i=0; i<list.length(); i++ ) {
                            JSONObject p = list.getJSONObject(i);
                            VirtualMachineProduct product = toProduct(p);
                            
                            if( product != null ) {
                                products.add(product);
                            }
                        }
                    }
                }
                catch( JSONException e ) {
                    std.error("listProducts(): Unable to identify expected values in JSON: " + e.getMessage());
                    throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for flavors: " + e.getMessage());
                }
                cachedProducts.put(provider.getContext().getRegionId(), Collections.unmodifiableCollection(products));
            }
            return products;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + CloudServers.class.getName() + ".listProducts()");
            }
        }
    }

    private transient Collection<Architecture> architectures;

    @Override
    public Iterable<Architecture> listSupportedArchitectures() throws InternalException, CloudException {
        if( architectures == null ) {
            ArrayList<Architecture> tmp = new ArrayList<Architecture>();

            tmp.add(Architecture.I32);
            tmp.add(Architecture.I64);
            architectures = Collections.unmodifiableList(tmp);
        }
        return architectures;
    }

    @Nonnull
    @Override
    public Iterable<ResourceStatus> listVirtualMachineStatus() throws InternalException, CloudException {
        ArrayList<ResourceStatus> status = new ArrayList<ResourceStatus>();

        for( VirtualMachine vm : listVirtualMachines() ) {
            status.add(new ResourceStatus(vm.getProviderVirtualMachineId(), vm.getCurrentState()));
        }
        return status;
    }

    @Override
    public @Nonnull Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        Logger std = RackspaceCloud.getLogger(CloudServers.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + CloudServers.class.getName() + ".listVirtualMachines()");
        }
        try {
            if( !provider.isMyRegion() ) {
                return Collections.emptyList();
            }
            RackspaceMethod method = new RackspaceMethod(provider);
            JSONObject ob = method.getServers("/servers", null);
            ArrayList<VirtualMachine> servers = new ArrayList<VirtualMachine>();
            
            try {
                if( ob.has("servers") ) {
                    JSONArray list = ob.getJSONArray("servers");
                    
                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject server = list.getJSONObject(i);
                        VirtualMachine vm = toVirtualMachine(server);
                        
                        if( vm != null ) {
                            servers.add(vm);
                        }
                        
                    }
                }
            }
            catch( JSONException e ) {
                std.error("listVirtualMachines(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for servers");
            }
            return servers;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + CloudServers.class.getName() + ".listVirtualMachines()");
            }
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void pause(@Nonnull String vmId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Rackspace does not support pause/unpause.");
    }

    @Override
    public void reboot(@Nonnull String vmId) throws CloudException, InternalException {
        Logger logger = RackspaceCloud.getLogger(CloudServers.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + CloudServers.class.getName() + ".reboot(" + vmId + ")");
        }
        try {
            HashMap<String,Object> json = new HashMap<String,Object>();
            HashMap<String,Object> action = new HashMap<String,Object>();
            
            action.put("type", "HARD");
            json.put("reboot", action);

            RackspaceMethod method = new RackspaceMethod(provider);
            
            method.postServers("/servers", vmId, new JSONObject(json));
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + CloudServers.class.getName() + ".reboot()");
            }            
        }
    }

    @Override
    public void resume(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Rackspace does not support suspend/resume of servers.");
    }

    @Override
    public void start(@Nonnull String vmId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Rackspace does not support start/stop of servers.");
    }

    @Override
    public void stop(@Nonnull String vmId) throws InternalException, CloudException {
        stop(vmId, false);
    }

    @Override
    public void stop(@Nonnull String vmId, boolean force) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Rackspace does not support start/stop of servers");
    }

    @Override
    public boolean supportsAnalytics() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsPauseUnpause(@Nonnull VirtualMachine vm) {
        return false;
    }

    @Override
    public boolean supportsStartStop(@Nonnull VirtualMachine vm) {
        return false;
    }

    @Override
    public boolean supportsSuspendResume(@Nonnull VirtualMachine vm) {
        return false;
    }

    @Override
    public void suspend(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Rackspace does not support suspend/resume of servers.");
    }

    @Override
    public void terminate(@Nonnull String vmId) throws InternalException, CloudException {
        Logger std = RackspaceCloud.getLogger(CloudServers.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + CloudServers.class.getName() + ".terminate(" + vmId + ")");
        }
        try {
            RackspaceMethod method = new RackspaceMethod(provider);
            long timeout = System.currentTimeMillis() + CalendarWrapper.HOUR;

            do {
                try {
                    method.deleteServers("/servers", vmId);
                    return;
                }
                catch( RackspaceException e ) {
                    if( e.getHttpCode() != HttpServletResponse.SC_CONFLICT ) {
                        throw e;
                    }
                }
                try { Thread.sleep(CalendarWrapper.MINUTE); }
                catch( InterruptedException e ) { /* ignore */ }
            } while( System.currentTimeMillis() < timeout );
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + CloudServers.class.getName() + ".terminate()");
            }
        }
    }

    @Override
    public void unpause(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Rackspace does not support pause/unpause.");
    }

    @Override
    public void updateTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
        // NO-OP
    }

    private @Nullable VirtualMachineProduct toProduct(@Nullable JSONObject json) throws JSONException, InternalException, CloudException {
        Logger std = RackspaceCloud.getLogger(CloudServers.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + CloudServers.class.getName() + ".toProduct(" + json + ")");
        }
        try {
            if( json == null ) {
                return null;
            }
            VirtualMachineProduct product = new VirtualMachineProduct();
            
            if( json.has("id") ) {
                product.setProviderProductId(json.getString("id"));
            }
            if( json.has("name") ) {
                product.setName(json.getString("name"));
            }
            if( json.has("description") ) {
                product.setDescription(json.getString("description"));
            }
            if( json.has("ram") ) {
                product.setRamSize(new Storage<Megabyte>(json.getInt("ram"), Storage.MEGABYTE));
            }
            if( json.has("disk") ) {
                product.setRootVolumeSize(new Storage<Gigabyte>(json.getInt("disk"), Storage.GIGABYTE));
            }
            product.setCpuCount(1);
            if( product.getProviderProductId() == null ) {
                return null;
            }
            if( product.getName() == null ) {
                product.setName(product.getProviderProductId());
            }
            if( product.getDescription() == null ) {
                product.setDescription(product.getName());
            }
            return product;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("enter - " + CloudServers.class.getName() + ".toProduct()");
            }            
        }
    }
    
    private @Nullable VirtualMachine toVirtualMachine(@Nullable JSONObject server) throws JSONException, InternalException, CloudException {
        Logger std = RackspaceCloud.getLogger(CloudServers.class, "std");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + CloudServers.class.getName() + ".toVirtualMachine(" + server + ")");
        }
        try {
            if( server == null ) {
                return null;
            }
            VirtualMachine vm = new VirtualMachine();
            
            vm.setCurrentState(VmState.RUNNING);
            vm.setArchitecture(Architecture.I64);
            vm.setClonable(false);
            vm.setCreationTimestamp(-1L);
            vm.setImagable(false);
            vm.setLastBootTimestamp(-1L);
            vm.setLastPauseTimestamp(-1L);
            vm.setPausable(false);
            vm.setPersistent(true);
            vm.setPlatform(Platform.UNKNOWN);
            vm.setRebootable(true);
            vm.setProviderOwnerId(provider.getContext().getAccountNumber());
            if( server.has("id") ) {
                vm.setProviderVirtualMachineId(String.valueOf(server.getLong("id")));
            }
            if( server.has("name") ) {
                vm.setName(server.getString("name"));
            }
            if( server.has("description") ) {
                vm.setDescription(server.getString("description"));
            }
            if( server.has("imageId") ) {
                vm.setProviderMachineImageId(server.getString("imageId"));
            }
            if( vm.getDescription() == null ) {
                HashMap<String,String> map = new HashMap<String,String>();
                
                if( server.has("metadata") ) {
                    JSONObject md = server.getJSONObject("metadata");
                    
                    if( md.has("dsnDescription") ) {
                        vm.setDescription(md.getString("dsnDescription"));
                    }
                    else if( md.has("dsnTrueImage") ) { // this will override the nonsense Rackspace is sending us
                        String imageId = md.getString("dsnTrueImage");
                        
                        if( imageId != null && imageId.length() > 0 ) {
                            vm.setProviderMachineImageId(imageId);
                        }
                    }
                    else if( md.has("Server Label") ) {
                        vm.setDescription(md.getString("Server Label"));
                    }
                    if( md.has("dsnName") ) {
                        String str = md.getString("dsnName");
                        
                        if( str != null && str.length() > 0 ) {
                            vm.setName(str);
                        }
                    }
                    if( md.has("dsnPlatform") ) {
                        try {
                            vm.setPlatform(Platform.valueOf(md.getString("dsnPlatform")));
                        }
                        catch( Throwable ignore ) {
                            // ignore
                        }
                    }
                    String[] keys = JSONObject.getNames(md);
                    
                    if( keys != null ) {
                        for( String key : keys ) {
                            String value = md.getString(key);
                            
                            if( value != null ) {
                                map.put(key, value);
                            }
                        }
                    }
                }
                if( vm.getDescription() == null ) {
                    if( vm.getName() == null ) {
                        vm.setName(vm.getProviderVirtualMachineId());
                    }
                    vm.setDescription(vm.getName());
                }
                if( server.has("hostId") ) {
                    map.put("host", server.getString("hostId"));
                }
                vm.setTags(map);
            }
            if( server.has("flavorId") ) {
                vm.setProductId(server.getString("flavorId"));
            }
            if( server.has("adminPass") ) {
                vm.setRootPassword(server.getString("adminPass"));
            }
            if( server.has("status") ) {
                String s = server.getString("status").toLowerCase();
                
                if( s.equals("active") ) {
                    vm.setCurrentState(VmState.RUNNING);
                }
                else if( s.equals("building") || s.equals("build") ) {
                    vm.setCurrentState(VmState.PENDING);
                }
                else if( s.equals("deleted") ) {
                    vm.setCurrentState(VmState.TERMINATED);
                }
                else if( s.equals("suspended") ) {
                    vm.setCurrentState(VmState.SUSPENDED);
                }
                else if( s.equals("error") ) {
                    return null;
                }
                else if( s.equals("reboot") || s.equals("hard_reboot") ) {
                    vm.setCurrentState(VmState.REBOOTING);
                }
                else {
                    std.warn("toVirtualMachine(): Unknown server state: " + s);
                    vm.setCurrentState(VmState.PENDING);
                }
            }
            if( server.has("addresses") ) {
                JSONObject addrs = server.getJSONObject("addresses");
                
                if( addrs.has("public") ) {
                    JSONArray arr = addrs.getJSONArray("public");
                    String[] addresses = new String[arr.length()];
                    
                    for( int i=0; i<arr.length(); i++ ) {
                        addresses[i] = arr.getString(i);
                    }
                    vm.setPublicIpAddresses(addresses);
                }
                else {
                    vm.setPublicIpAddresses(new String[0]);                    
                }
                if( addrs.has("private") ) {
                    JSONArray arr = addrs.getJSONArray("private");
                    String[] addresses = new String[arr.length()];
                    
                    for( int i=0; i<arr.length(); i++ ) {
                        addresses[i] = arr.getString(i);
                    }
                    vm.setPrivateIpAddresses(addresses);                    
                }
                else {
                    vm.setPrivateIpAddresses(new String[0]);                    
                }
            }
            vm.setProviderRegionId(provider.getContext().getRegionId());
            vm.setProviderDataCenterId(vm.getProviderRegionId() + "1");
            vm.setTerminationTimestamp(-1L);
            if( vm.getProviderVirtualMachineId() == null ) {
                return null;
            }
            vm.setImagable(vm.getCurrentState().equals(VmState.RUNNING));
            vm.setRebootable(vm.getCurrentState().equals(VmState.RUNNING));
            if( vm.getPlatform().equals(Platform.UNKNOWN) ) {
                Platform p = Platform.guess(vm.getName() + " " + vm.getDescription());
                
                if( p.equals(Platform.UNKNOWN) ) {
                    MachineImage img = provider.getComputeServices().getImageSupport().getMachineImage(vm.getProviderMachineImageId());
                    
                    if( img != null ) {
                        p = img.getPlatform();
                    }
                }
                vm.setPlatform(p);
            }
            return vm;
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + CloudServers.class.getName() + ".toVirtualMachine()");
            }
        }
    }

    private boolean serverExists(String name) throws InternalException, CloudException {
        for( VirtualMachine vm : listVirtualMachines() ) {
            if( vm.getName().equalsIgnoreCase(name) ) {
                return true;
            }
        }
        return false;
    }
    
    private String makeUpName(String name) throws InternalException, CloudException {
        StringBuilder str = new StringBuilder();
        char last = '\0';
        
        for( int i=0; i<name.length(); i++ ) {
            char c = name.charAt(i);
            
            if( c >= 'a' && c <= 'z' ) {
                if( last == '-' && !str.toString().endsWith("-") ) {
                    str.append('-');
                }
                str.append(c);
            }
            last = c;            
        }
        if( name.length() < 1 ) {
            name = "a";
        }
        String test = name;
        int idx = 1;
        
        while( serverExists(test) && idx<1000000 ) {
            test = name + "-" + (idx++);
        }
        return test;
    }
    
    private String validateName(String name, boolean safeName) throws InternalException, CloudException {
        if( safeName ) {
            name = name.toLowerCase().replaceAll(" ", "-");
        }
        if( name.length() > NAME_LIMIT ) {
            name = name.substring(0,NAME_LIMIT);
        }
        StringBuilder str = new StringBuilder();
        
        for( int i=0; i<name.length(); i++ ) {
            char c = name.charAt(i);
            
            if( !safeName ) {
                if( Character.isLetterOrDigit(c) || c == '-' || c == ' ') {     
                    if( str.length() > 0 || Character.isLetter(c) ) {
                        str.append(c);
                    }
                }
            }
            else {
                if( (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' ) {
                    if( str.length() > 0 || Character.isLetter(c) ) {
                        str.append(c);
                    }
                }
            }
        }
        if( str.length() < 1 ) {
            return makeUpName(name);
        }
        name = str.toString();
        while( !Character.isLetterOrDigit(name.charAt(name.length()-1)) ) {
            if( name.length() < 2 ) {
                return makeUpName(name);
            }
            name = name.substring(0, name.length()-1);
        }
        if( serverExists(name) ) {
            return makeUpName(name);
        }
        return name;
    }
}
