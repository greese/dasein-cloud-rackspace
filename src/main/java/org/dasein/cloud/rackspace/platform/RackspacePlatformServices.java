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

import org.dasein.cloud.platform.AbstractPlatformServices;
import org.dasein.cloud.rackspace.RackspaceCloud;

import javax.annotation.Nonnull;

public class RackspacePlatformServices extends AbstractPlatformServices {
    private RackspaceCloud provider;
    
    public RackspacePlatformServices(@Nonnull RackspaceCloud provider) { this.provider = provider; }
 
    @Override
    public @Nonnull CloudCDN getCDNSupport() {
        return new CloudCDN(provider);
    }
}
