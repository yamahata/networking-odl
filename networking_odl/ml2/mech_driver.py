# Copyright (c) 2013-2014 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import abc
import six

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils

from neutron.common import constants as n_const
from neutron.common import exceptions as n_exc
from neutron import context as neutron_context
from neutron.extensions import portbindings
from neutron.extensions import securitygroup as sg
from neutron.plugins.common import constants
from neutron.plugins.ml2 import driver_api
from neutron.plugins.ml2 import driver_context

from networking_odl.common import callback as odl_call
from networking_odl.common import client as odl_client
from networking_odl.common import constants as odl_const
from networking_odl.common import utils as odl_utils
from networking_odl.db import journal_db
from networking_odl.ml2 import state_syncer
from networking_odl.openstack.common._i18n import _LE

LOG = logging.getLogger(__name__)

not_found_exception_map = {odl_const.ODL_NETWORKS: n_exc.NetworkNotFound,
                           odl_const.ODL_SUBNETS: n_exc.SubnetNotFound,
                           odl_const.ODL_PORTS: n_exc.PortNotFound,
                           odl_const.ODL_SGS: sg.SecurityGroupNotFound,
                           odl_const.ODL_SG_RULES:
                               sg.SecurityGroupRuleNotFound}


@six.add_metaclass(abc.ABCMeta)
class ResourceFilterBase(object):
    @staticmethod
    @abc.abstractmethod
    def filter_create_attributes(resource, context):
        pass

    @staticmethod
    @abc.abstractmethod
    def filter_update_attributes(resource, context):
        pass

    @staticmethod
    @abc.abstractmethod
    def filter_create_attributes_with_plugin(resource, plugin, dbcontext):
        pass


class NetworkFilter(ResourceFilterBase):
    @staticmethod
    def filter_create_attributes(network, context):
        """Filter out network attributes not required for a create."""
        odl_utils.try_del(network, ['status', 'subnets'])

    @staticmethod
    def filter_update_attributes(network, context):
        """Filter out network attributes for an update operation."""
        odl_utils.try_del(network, ['id', 'status', 'subnets', 'tenant_id'])

    @classmethod
    def filter_create_attributes_with_plugin(cls, network, plugin, dbcontext):
        context = driver_context.NetworkContext(plugin, dbcontext, network)
        cls.filter_create_attributes(network, context)


class SubnetFilter(ResourceFilterBase):
    @staticmethod
    def filter_create_attributes(subnet, context):
        """Filter out subnet attributes not required for a create."""
        pass

    @staticmethod
    def filter_update_attributes(subnet, context):
        """Filter out subnet attributes for an update operation."""
        odl_utils.try_del(subnet, ['id', 'network_id', 'ip_version', 'cidr',
                          'allocation_pools', 'tenant_id'])

    @classmethod
    def filter_create_attributes_with_plugin(cls, subnet, plugin, dbcontext):
        context = driver_context.SubnetContext(subnet, plugin, dbcontext)
        cls.filter_create_attributes(subnet, context)


class PortFilter(ResourceFilterBase):
    @staticmethod
    def _add_security_groups(port, context):
        """Populate the 'security_groups' field with entire records."""
        dbcontext = context._plugin_context
        groups = [context._plugin.get_security_group(dbcontext, sg)
                  for sg in port['security_groups']]
        port['security_groups'] = groups

    @classmethod
    def filter_create_attributes(cls, port, context):
        """Filter out port attributes not required for a create."""
        cls._add_security_groups(port, context)
        # TODO(kmestery): Converting to uppercase due to ODL bug
        # https://bugs.opendaylight.org/show_bug.cgi?id=477
        port['mac_address'] = port['mac_address'].upper()
        odl_utils.try_del(port, ['status'])

        # NOTE(yamahata): work around for port creation for router
        # tenant_id=''(empty string) is passed when port is created
        # by l3 plugin internally for router.
        # On the other hand, ODL doesn't accept empty string for tenant_id.
        # In that case, deduce tenant_id from network_id for now.
        # Right fix: modify Neutron so that don't allow empty string
        # for tenant_id even for port for internal use.
        # TODO(yamahata): eliminate this work around when neutron side
        # is fixed
        # assert port['tenant_id'] != ''
        if port['tenant_id'] == '':
            LOG.debug('empty string was passed for tenant_id: %s(port)', port)
            port['tenant_id'] = context._network_context._network['tenant_id']

    @classmethod
    def filter_update_attributes(cls, port, context):
        """Filter out port attributes for an update operation."""
        cls._add_security_groups(port, context)
        odl_utils.try_del(port, ['network_id', 'id', 'status', 'mac_address',
                          'tenant_id', 'fixed_ips'])

    @classmethod
    def filter_create_attributes_with_plugin(cls, port, plugin, dbcontext):
        network = plugin.get_network(dbcontext, port['network_id'])
        # TODO(yamahata): port binding
        binding = {}
        context = driver_context.PortContext(
            plugin, dbcontext, port, network, binding, None)
        cls.filter_create_attributes(port, context)


class SecurityGroupFilter(ResourceFilterBase):
    @staticmethod
    def filter_create_attributes(sg, context):
        """Filter out security-group attributes not required for a create."""
        pass

    @staticmethod
    def filter_update_attributes(sg, context):
        """Filter out security-group attributes for an update operation."""
        pass

    @staticmethod
    def filter_create_attributes_with_plugin(sg, plugin, dbcontext):
        pass


class SecurityGroupRuleFilter(ResourceFilterBase):
    @staticmethod
    def filter_create_attributes(sg_rule, context):
        """Filter out sg-rule attributes not required for a create."""
        pass

    @staticmethod
    def filter_update_attributes(sg_rule, context):
        """Filter out sg-rule attributes for an update operation."""
        pass

    @staticmethod
    def filter_create_attributes_with_plugin(sg_rule, plugin, dbcontext):
        pass


class OpenDaylightDriver(object):

    """OpenDaylight Python Driver for Neutron.

    This code is the backend implementation for the OpenDaylight ML2
    MechanismDriver for OpenStack Neutron.
    """
    FILTER_MAP = {
        odl_const.ODL_NETWORKS: NetworkFilter,
        odl_const.ODL_SUBNETS: SubnetFilter,
        odl_const.ODL_PORTS: PortFilter,
        odl_const.ODL_SGS: SecurityGroupFilter,
        odl_const.ODL_SG_RULES: SecurityGroupRuleFilter,
    }
    out_of_sync = True

    def __init__(self):
        LOG.debug("Initializing OpenDaylight ML2 driver")
        self.client = odl_client.OpenDaylightRestClient(
            cfg.CONF.ml2_odl.url,
            cfg.CONF.ml2_odl.username,
            cfg.CONF.ml2_odl.password,
            cfg.CONF.ml2_odl.timeout
        )
        self.journal = journal_db.JournalDb()
        self.sec_handler = odl_call.OdlSecurityGroupsHandler(self)
        self.vif_details = {portbindings.CAP_PORT_FILTER: True}
        self.state_syncer = state_syncer.OpenDaylightStateSyncer(self.journal)

    def journal_resource(self, context,
                         object_type, obj_id, operation, resource):
        resource_str = self.client.serialize(resource)
        self.journal.log(context, object_type, obj_id, operation, resource_str)

    def journal(self, operation, object_type, context):
        """Journal request to ODL."""
        obj_id = context.current['id']
        if operation == odl_const.ODL_DELETE:
            resource = {}
        else:
            filter_cls = self.FILTER_MAP[object_type]
            if operation == odl_const.ODL_CREATE:
                attr_filter = filter_cls.filter_create_attributes
            elif operation == odl_const.ODL_UPDATE:
                attr_filter = filter_cls.filter_update_attributes
            resource = context.current.copy()
            attr_filter(resource, context)
        self.journal_resource(context,
                              object_type, obj_id, operation, resource)

    def synchronize(self, operation, object_type, context):
        """Synchronize ODL with Neutron following a configuration change."""
        # TODO(yamahata): try to synchronize this resource in ahead of
        # state_syncer in order to reduce latency in case of good condition
        # self.sync_single_resource(operation, object_type, context)
        # if in case of complex situation, punt this task to state_syncer
        # self.sync_single_resource()
        self.state_syncer.wakeup()

    # TODO(yamahata): Unused for now. Later fix up this method for
    # optimization for latency
    def sync_single_resource(self, operation, object_type, context):
        """Sync over a single resource from Neutron to OpenDaylight.

        Handle syncing a single operation over to OpenDaylight, and correctly
        filter attributes out which are not required for the requisite
        operation (create or update) being handled.
        """
        LOG.debug('not implemented yet')
        return

        # Convert underscores to dashes in the URL for ODL
        object_type_url = object_type.replace('_', '-')
        try:
            obj_id = context.current['id']
            if operation == odl_const.ODL_DELETE:
                self.out_of_sync |= not self.client.try_delete(
                    object_type_url + '/' + obj_id)
            else:
                filter_cls = self.FILTER_MAP[object_type]
                if operation == odl_const.ODL_CREATE:
                    urlpath = object_type_url
                    method = 'post'
                    attr_filter = filter_cls.filter_create_attributes
                elif operation == odl_const.ODL_UPDATE:
                    urlpath = object_type_url + '/' + obj_id
                    method = 'put'
                    attr_filter = filter_cls.filter_update_attributes
                resource = context.current.copy()
                attr_filter(resource, context)
                self.client.sendjson(method, urlpath,
                                     {object_type_url[:-1]: resource})
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("Unable to perform %(operation)s on "
                              "%(object_type)s %(object_id)s"),
                          {'operation': operation,
                           'object_type': object_type,
                           'object_id': obj_id})
                self.out_of_sync = True

    def sync_from_callback(self, operation, object_type, res_id, resource):
        # TODO(yamahata): utilize ML2 security group driver and optimize
        # synchronize path in order to reduce latancy
        assert res_id
        context = neutron_context.get_admin_context()
        self.journal_resource(context,
                              object_type, res_id, operation, resource)
        self.journal.wakeup()

    def bind_port(self, port_context):
        """Set binding for all valid segments

        """

        valid_segment = None
        for segment in port_context.segments_to_bind:
            if self._check_segment(segment):
                valid_segment = segment
                break

        if valid_segment:
            vif_type = self._get_vif_type(port_context)
            LOG.debug("Bind port %(port)s on network %(network)s with valid "
                      "segment %(segment)s and VIF type %(vif_type)r.",
                      {'port': port_context.current['id'],
                       'network': port_context.network.current['id'],
                       'segment': valid_segment, 'vif_type': vif_type})

            port_context.set_binding(
                segment[driver_api.ID], vif_type,
                self.vif_details,
                status=n_const.PORT_STATUS_ACTIVE)

    def _check_segment(self, segment):
        """Verify a segment is valid for the OpenDaylight MechanismDriver.

        Verify the requested segment is supported by ODL and return True or
        False to indicate this to callers.
        """

        network_type = segment[driver_api.NETWORK_TYPE]
        return network_type in [constants.TYPE_LOCAL, constants.TYPE_GRE,
                                constants.TYPE_VXLAN, constants.TYPE_VLAN]

    def _get_vif_type(self, port_context):
        """Get VIF type string for given PortContext

        Dummy implementation: it always returns following constant.
        neutron.extensions.portbindings.VIF_TYPE_OVS
        """

        return portbindings.VIF_TYPE_OVS
