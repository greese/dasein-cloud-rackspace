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

package org.dasein.cloud.rackspace.network;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.LbAlgorithm;
import org.dasein.cloud.network.LbListener;
import org.dasein.cloud.network.LbProtocol;
import org.dasein.cloud.network.LoadBalancer;
import org.dasein.cloud.network.LoadBalancerAddressType;
import org.dasein.cloud.network.LoadBalancerState;
import org.dasein.cloud.network.LoadBalancerSupport;
import org.dasein.cloud.rackspace.RackspaceCloud;
import org.dasein.cloud.rackspace.RackspaceException;
import org.dasein.cloud.rackspace.RackspaceMethod;
import org.dasein.util.CalendarWrapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class CloudLoadBalancers implements LoadBalancerSupport {
    private RackspaceCloud provider;
    
    CloudLoadBalancers(RackspaceCloud provider) { this.provider = provider; }

    @Override
    public void addDataCenters(String toLoadBalancerId, String... dataCenterIdsToAdd) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No support for data-center constrained load balancers");
    }

    @Override
    public void addServers(String toLoadBalancerId, String... serverIdsToAdd) throws CloudException, InternalException {
        Logger logger = RackspaceCloud.getLogger(CloudLoadBalancers.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + CloudLoadBalancers.class.getName() + ".addServers(" + toLoadBalancerId + "," + serverIdsToAdd + ")");
        }
        try {
            ArrayList<HashMap<String,Object>> nodes = new ArrayList<HashMap<String,Object>>();
            LoadBalancer lb = getLoadBalancer(toLoadBalancerId);
            int port = -1;
            
            if( lb == null ) {
                logger.error("addServers(): No such load balancer: " + toLoadBalancerId);
                throw new CloudException("No such load balancer: " + toLoadBalancerId);
            }
            LbListener[] listeners = lb.getListeners();
            
            if( listeners != null ) {
                for( LbListener listener : listeners ) {
                    port = listener.getPrivatePort();
                    break;
                }
                if( port == -1 ) {
                    for( LbListener listener : listeners ) {
                        port = listener.getPublicPort();
                        break;
                    }                
                }
            }
            if( port == -1 ) {
                if( lb.getPublicPorts() != null && lb.getPublicPorts().length > 0 ) {
                    port = lb.getPublicPorts()[0];
                }
                if( port == -1 ) {
                    logger.error("addServers(): Could not determine a proper private port for mapping");
                    throw new CloudException("No port understanding exists for this load balancer");
                }
            }
            for( String id : serverIdsToAdd ) {
                if( logger.isTraceEnabled() ) {
                    logger.trace("addServers(): Adding " + id + "...");
                }
                VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(id);
                
                if( vm == null ) {
                    logger.error("addServers(): Failed to add " + id + " because it does not exist");
                    throw new CloudException("No such server: " + id);
                }
                String address = null;
                
                if( vm.getProviderRegionId().equals(provider.getContext().getRegionId()) ) {
                    for( String addr : vm.getPrivateIpAddresses() ) {
                        address = addr;
                        break;
                    }
                }
                if( address == null ) {
                    for( String addr : vm.getPublicIpAddresses() ) {
                        address = addr;
                        break;
                    }                
                }
                if( address == null ) {
                    logger.error("addServers(): No address exists for mapping the load balancer to this server");
                    throw new CloudException("The virtual machine " + id + " has no mappable addresses");
                }
                if( logger.isDebugEnabled() ) {
                    logger.debug("addServers(): Mapping IP is: " + address);
                }
                HashMap<String,Object> node = new HashMap<String,Object>();
                
                
                node.put("address", address);
                node.put("condition", "ENABLED");
                node.put("port", port);
                nodes.add(node);
            }
            if( !nodes.isEmpty() ) {
                HashMap<String,Object> json = new HashMap<String,Object>();
            
                json.put("nodes", nodes);
                RackspaceMethod method = new RackspaceMethod(provider);
                
                if( logger.isTraceEnabled() ) {
                    logger.debug("addServers(): Calling cloud...");
                }
                try {
                    method.postLoadBalancers("/loadbalancers", toLoadBalancerId + "/nodes", new JSONObject(json));
                }
                catch( RackspaceException e ) {
                    if( e.getHttpCode() == 422 && nodes.size() == 1 ) {
                        nodes.clear();
                        for( String id : serverIdsToAdd ) {
                            if( logger.isTraceEnabled() ) {
                                logger.trace("addServers(): Adding " + id + "...");
                            }
                            VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(id);
                            
                            if( vm == null ) {
                                logger.error("addServers(): Failed to add " + id + " because it does not exist");
                                throw new CloudException("No such server:" + id);
                            }
                            String address = null;
                            
                            for( String addr : vm.getPublicIpAddresses() ) {
                                address = addr;
                                break;
                            }
                            if( address == null ) {
                                logger.error("addServers(): No public address exists for mapping the load balancer to this server");
                                throw new CloudException("The virtual machine " + id + " has no publicly mappable addresses");
                            }
                            if( logger.isDebugEnabled() ) {
                                logger.debug("addServers(): Mapping IP is: " + address);
                            }
                            HashMap<String,Object> node = new HashMap<String,Object>();
                            
                            
                            node.put("address", address);
                            node.put("condition", "ENABLED");
                            node.put("port", port);
                            nodes.add(node);
                        } 
                        json.clear();
                        json.put("nodes", nodes);
                        if( logger.isTraceEnabled() ) {
                            logger.debug("addServers(): Attemptign with public IP...");
                        }
                        method.postLoadBalancers("/loadbalancers", toLoadBalancerId + "/nodes", new JSONObject(json));
                    }
                }
                if( logger.isTraceEnabled() ) {
                    logger.debug("addServers(): Done.");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + CloudLoadBalancers.class.getName() + ".addServers()");
            }
        }
    }

    @Override
    public String create(String name, String description, String addressId, String[] dataCenterIds, LbListener[] listeners, String[] serverIds) throws CloudException, InternalException {
        Logger logger = RackspaceCloud.getLogger(CloudLoadBalancers.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + CloudLoadBalancers.class.getName() + ".create(" + name + "," + description + "," + addressId + "," + dataCenterIds + "," + listeners + "," + serverIds + ")");
        }
        try {
            if( listeners == null || listeners.length < 1 ) {
                logger.error("create(): Call failed to specify any listeners");
                throw new CloudException("Rackspace requires exactly one listener");
            }
            HashMap<String,Object> lb = new HashMap<String,Object>();
            
            lb.put("name", name);
            lb.put("port", listeners[0].getPublicPort());
            if( listeners[0].getNetworkProtocol().equals(LbProtocol.HTTP) ) {
                lb.put("protocol", "HTTP");
            }
            else if( listeners[0].getNetworkProtocol().equals(LbProtocol.HTTPS) ) {
                lb.put("protocol", "HTTPS");
            }
            else if( listeners[0].getNetworkProtocol().equals(LbProtocol.RAW_TCP) ) {
                lb.put("protocol", matchProtocol(listeners[0].getPublicPort()));
            }
            else {
                logger.error("create(): Invalid protocol: " + listeners[0].getNetworkProtocol());
                throw new CloudException("Unsupported protocol: " + listeners[0].getNetworkProtocol());
            }
            if( listeners[0].getAlgorithm().equals(LbAlgorithm.LEAST_CONN) ) {
                lb.put("algorithm", "LEAST_CONNECTIONS");
            }
            else if( listeners[0].getAlgorithm().equals(LbAlgorithm.ROUND_ROBIN) ) {
                lb.put("algorithm", "ROUND_ROBIN");
            }
            else {
                logger.error("create(): Invalid algorithm: " + listeners[0].getAlgorithm());
                throw new CloudException("Unsupported algorithm: " + listeners[0].getAlgorithm());
            }
            ArrayList<Map<String,Object>> ips = new ArrayList<Map<String,Object>>();
            HashMap<String,Object> ip = new HashMap<String,Object>();
            
            ip.put("type", "PUBLIC");
            ips.add(ip);
            lb.put("virtualIps", ips);
            
            ArrayList<Map<String,Object>> nodes = new ArrayList<Map<String,Object>>();
            
            for( String id : serverIds ) {
                VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(id);
                
                if( vm != null ) {
                    String address = null;
                    
                    if( vm.getProviderRegionId().equals(provider.getContext().getRegionId()) ) {
                        for( String addr : vm.getPrivateIpAddresses() ) {
                            address = addr;
                            break;
                        }
                    }
                    if( address == null ) {
                        for( String addr : vm.getPublicIpAddresses() ) {
                            address = addr;
                            break;
                        }                
                    }
                    if( address != null ) {
                        HashMap<String,Object> node = new HashMap<String,Object>();
                    
                        node.put("address", address);
                        node.put("condition", "ENABLED");
                        node.put("port", listeners[0].getPrivatePort());
                        nodes.add(node);
                    }
                }
            }
            if( nodes.isEmpty() ) {
                logger.error("create(): Rackspace requires at least one node assignment");
                throw new CloudException("Rackspace requires at least one node assignment");
            }
            lb.put("nodes", nodes);
            
            HashMap<String,Object> json = new HashMap<String,Object>();
            
            json.put("loadBalancer", lb);
            RackspaceMethod method = new RackspaceMethod(provider);
            
            if( logger.isTraceEnabled() ) {
                logger.trace("create(): Posting new load balancer data...");
            }
            JSONObject result = method.postLoadBalancers("/loadbalancers", null, new JSONObject(json));
            
            if( result == null ) {
                logger.error("create(): Method executed successfully, but no load balancer was created");
                throw new CloudException("Method executed successfully, but no load balancer was created");
            }
            try{
                if( result.has("loadBalancer") ) {
                    JSONObject ob = result.getJSONObject("loadBalancer");
                    
                    if( ob != null ) {
                        return ob.getString("id");
                    }
                }
                logger.error("create(): Method executed successfully, but no load balancer was found in JSON");                        
                throw new CloudException("Method executed successfully, but no load balancer was found in JSON");                        
            }
            catch( JSONException e ) {
                logger.error("create(): Failed to identify a load balancer ID in the cloud response: " + e.getMessage());
                throw new CloudException("Failed to identify a load balancer ID in the cloud response: " + e.getMessage());
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + CloudLoadBalancers.class.getName() + ".create()");
            }            
        }
    }

    private String matchProtocol(int port) throws CloudException, InternalException {
        RackspaceMethod method = new RackspaceMethod(provider);
        JSONObject ob = method.getLoadBalancers("/loadbalancers", "protocols");
        
        if( ob == null ) {
            return "TCP";
        }
        else {
            if( ob.has("protocols") ) {
                try {
                    JSONArray list = ob.getJSONArray("protocols");
                    
                    for( int i=0; i<list.length(); i++ ) {
                        JSONObject p = list.getJSONObject(i);
                        
                        if( p.has("port") && p.getInt("port") == port ) {
                            return p.getString("name");
                        }
                    }
                }
                catch( JSONException e ) {
                    throw new CloudException("Unable to parse protocols from Rackspace: " + e.getMessage());
                }
            }
            return "TCP";
        }
    }

    @Override
    public LoadBalancer getLoadBalancer(String loadBalancerId) throws CloudException, InternalException {
        Logger logger = RackspaceCloud.getLogger(CloudLoadBalancers.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + CloudLoadBalancers.class.getName() + ".getLoadBalancer(" + loadBalancerId + ")");
        }
        try {
            RackspaceMethod method = new RackspaceMethod(provider);
            JSONObject ob = method.getLoadBalancers("/loadbalancers", loadBalancerId);  
            
            if( ob == null ) {
                return null;
            }
            Iterable<VirtualMachine> vms = provider.getComputeServices().getVirtualMachineSupport().listVirtualMachines();
            
            try {
                if( ob.has("loadBalancer") ) {
                    LoadBalancer lb = toLoadBalancer(ob.getJSONObject("loadBalancer"), vms);
                        
                    if( lb != null ) {
                        return lb;
                    }
                }
                return null;
            }
            catch( JSONException e ) {
                logger.error("listLoadBalancers(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for load balancers: " + e.getMessage());
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + CloudLoadBalancers.class.getName() + ".getLoadBalancer()");
            }
        }
    }

    @Override
    public LoadBalancerAddressType getAddressType() throws CloudException, InternalException {
        return LoadBalancerAddressType.IP;
    }

    @Override
    public int getMaxPublicPorts() throws CloudException, InternalException {
        return 1;
    }

    @Override
    public String getProviderTermForLoadBalancer(Locale locale) {
        return "load balancer";
    }

    static private transient Collection<LbAlgorithm> supportedAlgorithms;
    
    @Override
    public Iterable<LbAlgorithm> listSupportedAlgorithms() throws CloudException, InternalException {
        if( supportedAlgorithms == null ) {
            ArrayList<LbAlgorithm> algorithms = new ArrayList<LbAlgorithm>();
            
            algorithms.add(LbAlgorithm.ROUND_ROBIN);
            algorithms.add(LbAlgorithm.LEAST_CONN);
            supportedAlgorithms = Collections.unmodifiableList(algorithms);
        }
        return supportedAlgorithms;
    }

    @Override
    public @Nonnull Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return Collections.singletonList(IPVersion.IPV4);
    }

    static private transient Collection<LbProtocol> supportedProtocols;
    
    @Override
    public Iterable<LbProtocol> listSupportedProtocols() throws CloudException, InternalException {
        if( supportedProtocols == null ) {
            ArrayList<LbProtocol> protocols = new ArrayList<LbProtocol>();
            
            protocols.add(LbProtocol.HTTP);
            protocols.add(LbProtocol.HTTPS);
            supportedProtocols = Collections.unmodifiableList(protocols);
        }
        return supportedProtocols;
    }

    @Override
    public boolean isAddressAssignedByProvider() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isDataCenterLimited() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public boolean requiresListenerOnCreate() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean requiresServerOnCreate() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return (provider.testContext() != null);
    }

    @Override
    public boolean supportsMonitoring() throws CloudException, InternalException {
        return false;
    }

    @Override
    public Iterable<LoadBalancer> listLoadBalancers() throws CloudException, InternalException {
        Logger logger = RackspaceCloud.getLogger(CloudLoadBalancers.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + CloudLoadBalancers.class.getName() + ".listLoadBalancers()");
        }
        try {
            RackspaceMethod method = new RackspaceMethod(provider);
            JSONObject ob = method.getLoadBalancers("/loadbalancers", null);  
            
            try {
                ArrayList<LoadBalancer> loadBalancers = new ArrayList<LoadBalancer>();
                
                if( ob.has("loadBalancers") ) {
                    JSONArray lbs = ob.getJSONArray("loadBalancers");
                    
                    if( lbs.length() > 0 ) {
                        Iterable<VirtualMachine> vms = provider.getComputeServices().getVirtualMachineSupport().listVirtualMachines();

                        for( int i=0; i<lbs.length(); i++ ) {
                            JSONObject tmp = lbs.getJSONObject(i);
                            
                            if( tmp.has("id") ) {
                                JSONObject actual = method.getLoadBalancers("/loadbalancers", tmp.getString("id"));
                                
                                if( actual != null && actual.has("loadBalancer") ) {
                                    LoadBalancer lb = this.toLoadBalancer(actual.getJSONObject("loadBalancer"), vms);
                                
                                    if( lb != null ) {
                                        loadBalancers.add(lb);
                                    }
                                }
                            }
                        }
                    }
                }
                return loadBalancers;
            }
            catch( JSONException e ) {
                logger.error("listLoadBalancers(): Unable to identify expected values in JSON: " + e.getMessage());
                throw new CloudException(CloudErrorType.COMMUNICATION, 200, "invalidJson", "Missing JSON element for load balancers: " + e.getMessage());
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + CloudLoadBalancers.class.getName() + ".listLoadBalancers()");
            }
        }
    }

    @Override
    public void remove(String loadBalancerId) throws CloudException, InternalException {
        Logger logger = RackspaceCloud.getLogger(CloudLoadBalancers.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + CloudLoadBalancers.class.getName() + ".remove(" + loadBalancerId + ")");
        }
        try {
            RackspaceMethod method = new RackspaceMethod(provider);
            long timeout = System.currentTimeMillis() + CalendarWrapper.HOUR;

            do {
                try {
                    method.deleteLoadBalancers("/loadbalancers", loadBalancerId);
                    return;
                }
                catch( RackspaceException e ) {
                    if( e.getHttpCode() != HttpServletResponse.SC_CONFLICT || e.getHttpCode() == 422 ) {
                        throw e;
                    }
                }
                try { Thread.sleep(CalendarWrapper.MINUTE); }
                catch( InterruptedException e ) { }
            } while( System.currentTimeMillis() < timeout );
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + CloudLoadBalancers.class.getName() + ".remove()");
            }
        }
    }

    static private class Node {
        public String nodeId;
        public String address;
    }
    
    public Collection<Node> getNodes(String loadBalancerId) throws CloudException, InternalException {
        ArrayList<Node> nodes = new ArrayList<Node>();
        RackspaceMethod method = new RackspaceMethod(provider);
        JSONObject response = method.getLoadBalancers("/loadbalancers", loadBalancerId + "/nodes");

        if( response != null && response.has("nodes") ) {
            try {
                JSONArray arr = response.getJSONArray("nodes");
                
                for( int i=0; i<arr.length(); i++ ) {
                    JSONObject node = arr.getJSONObject(i);
                    Node n = new Node();
                    
                    n.nodeId = node.getString("id");
                    n.address = node.getString("address");
                    nodes.add(n);
                }
            }
            catch( JSONException e ) {
                throw new CloudException("Unable to read nodes: " + e.getMessage());
            }
        }
        return nodes;
    }
    
    private Collection<String> mapNodes(String loadBalancerId, String[] serverIds) throws CloudException, InternalException {
        TreeSet<String> nodeIds = new TreeSet<String>();

        if( serverIds != null && serverIds.length > 0 ) {
            Collection<Node> nodes = getNodes(loadBalancerId);
            
            for( String serverId : serverIds ) {
                VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(serverId);
                
                if( vm != null ) {
                    boolean there = false;
                    
                    if( vm.getProviderRegionId().equals(provider.getContext().getRegionId()) ) {
                        String[] addrs = vm.getPrivateIpAddresses();
                        
                        if( addrs != null ) {
                            for( String addr : addrs ) {
                                for( Node n : nodes ) {
                                    if( n.address.equals(addr) ) {
                                        nodeIds.add(n.nodeId);
                                        there = true;
                                        break;
                                    }
                                }
                                if( there ) {
                                    break;
                                }
                            }
                        }
                    }
                    if( !there ) {
                        String[] addrs = vm.getPublicIpAddresses();
                        
                        if( addrs != null ) {
                            for( String addr : addrs ) {
                                for( Node n : nodes ) {
                                    if( n.address.equals(addr) ) {
                                        nodeIds.add(n.nodeId);
                                        there = true;
                                        break;
                                    }
                                }
                                if( there ) {
                                    break;
                                }
                            }
                        }                        
                    }
                }
            }
        }
        return nodeIds;
    }
    
    @Override
    public void removeDataCenters(String fromLoadBalancerId, String... dataCenterIdsToRemove) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No data center constraints in Rackspace");
    }

    @Override
    public void removeServers(String fromLoadBalancerId, String... serverIdsToRemove) throws CloudException, InternalException {
        Collection<String> nodeIds = mapNodes(fromLoadBalancerId, serverIdsToRemove);
        
        if( nodeIds.size() < 1 ) {
            return;
        }
        StringBuilder nodeString = new StringBuilder();
        
        for( String id : nodeIds ) {
            if( nodeString.length() > 0 ) {
                nodeString.append("&");
            }
            nodeString.append("nodeId=" + id);
        }
        RackspaceMethod method = new RackspaceMethod(provider);
        
        method.deleteLoadBalancers("/loadbalancers", fromLoadBalancerId + "/nodes?" + nodeString.toString());
    }
    
    private LoadBalancer toLoadBalancer(JSONObject json, Iterable<VirtualMachine> possibleNodes) throws JSONException, CloudException {
        LoadBalancer loadBalancer = new LoadBalancer();
        
        loadBalancer.setProviderDataCenterIds(new String[] { provider.getContext().getRegionId() + "1" });
        loadBalancer.setProviderOwnerId(provider.getContext().getAccountNumber());
        loadBalancer.setProviderRegionId(provider.getContext().getRegionId());
        loadBalancer.setAddressType(LoadBalancerAddressType.IP);
        if( json.has("id") ) {
            loadBalancer.setProviderLoadBalancerId(json.getString("id"));
        }
        if( json.has("name") ) {
            loadBalancer.setName(json.getString("name"));
        }
        if( json.has("created") ) {
            JSONObject ob = json.getJSONObject("created");
            
            if( ob.has("time") ) {
                loadBalancer.setCreationTimestamp(provider.parseTimestamp(ob.getString("time")));
            }
        }
        if( json.has("status") ) {
            String s = json.getString("status").toLowerCase();
            
            if( s.equals("active") ) {
                loadBalancer.setCurrentState(LoadBalancerState.ACTIVE);                
            }
            else {
                loadBalancer.setCurrentState(LoadBalancerState.PENDING);
            }
        }
        if( json.has("virtualIps") ) {
            JSONArray arr = json.getJSONArray("virtualIps");
            
            for( int i=0; i<arr.length(); i++ ) {
                JSONObject ob = arr.getJSONObject(i);
                
                if( ob.has("ipVersion") && ob.getString("ipVersion").equalsIgnoreCase("ipv4") ) {
                    if( ob.has("address") ) {
                        loadBalancer.setAddress(ob.getString("address"));
                        break;
                    }
                }
            }
        }
        int privatePort = -1;
        
        loadBalancer.setProviderServerIds(new String[0]);
        if( json.has("nodes") ) {
            ArrayList<String> nodes = new ArrayList<String>();
            JSONArray arr = json.getJSONArray("nodes");
            
            for( int i=0; i<arr.length(); i++ ) {
                JSONObject ob = arr.getJSONObject(i);
                
                if( ob.has("address") ) {
                    String address = ob.getString("address");
                    VirtualMachine node = null;
                    
                    for( VirtualMachine vm : possibleNodes ) {
                        String[] addrs = vm.getPublicIpAddresses();
                        
                        if( addrs != null ) {
                            for( String addr : addrs ){
                                if( address.equals(addr) ) {
                                    node = vm;
                                    break;
                                }
                            }
                        }
                        if( node == null ) {
                            addrs = vm.getPrivateIpAddresses();
                            if( addrs != null ) {
                                for( String addr : addrs ){
                                    if( address.equals(addr) ) {
                                        node = vm;
                                        break;
                                    }
                                }
                            }                            
                        }
                    }
                    if( node != null ) {
                        nodes.add(node.getProviderVirtualMachineId());
                    }
                }
                else if( ob.has("port") ) {
                    privatePort = ob.getInt("port");
                }
            }
            loadBalancer.setProviderServerIds(nodes.toArray(new String[nodes.size()]));
        }
        if( loadBalancer.getProviderLoadBalancerId() == null ) {
            return null;
        }
        int port = -1;
        
        if( json.has("port") ) {
            port = json.getInt("port");
            if( privatePort == -1 ) {
                privatePort = port;
            }
        }
        loadBalancer.setPublicPorts(new int[] { port });

        LbProtocol protocol = LbProtocol.RAW_TCP;
        
        if( json.has("protocol") ) {
            String p = json.getString("protocol");
            
            if( p.equals("HTTP") ) {
                protocol = LbProtocol.HTTP;
            }
            else if( p.equals("HTTPS") ) {
                protocol = LbProtocol.HTTPS;
            }
            else if( p.equals("AJP") ) {
                protocol = LbProtocol.AJP;
            }
        }
        
        LbAlgorithm algorithm = LbAlgorithm.ROUND_ROBIN;
        
        if( json.has("algorithm") ) {
            String a = json.getString("algorithm").toLowerCase();
            
            if( a.equals("round_robin") ) {
                algorithm = LbAlgorithm.ROUND_ROBIN;
            }
            else if( a.equals("least_connections") ) {
                algorithm = LbAlgorithm.LEAST_CONN;
            }
        }
        LbListener l = new LbListener();
        
        l.setAlgorithm(algorithm);
        l.setNetworkProtocol(protocol);
        l.setPublicPort(port);
        l.setPrivatePort(privatePort);
        loadBalancer.setListeners(new LbListener[] { l });
        if( loadBalancer.getName() == null ) {
            loadBalancer.setName(loadBalancer.getProviderLoadBalancerId());
        }
        if( loadBalancer.getDescription() == null ) {
            loadBalancer.setDescription(loadBalancer.getName());
        }
        return loadBalancer;
    }
}
