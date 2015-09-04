# Copyright 2015 Intel Corporation.
# Copyright 2015 Isaku Yamahata <isaku.yamahata at intel com>
#                               <isaku.yamahata at gmail com>
# All Rights Reserved.
#
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

import eventlet
from eventlet import queue
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils
import requests

from neutron.common import utils
from neutron import context as neutron_context

from networking_odl.common import client as odl_client
from networking_odl.common import constants as odl_const
from networking_odl.openstack.common._i18n import _LE


LOG = logging.getLogger(__name__)


class OpenDaylightStateSyncer(object):
    def __init__(self, journal):
        LOG.debug("Initializing OpenDaylight State Syncer")
        self._out_of_sync = True
        self.client = odl_client.OpenDaylightRestClient(
            cfg.CONF.ml2_odl.url,
            cfg.CONF.ml2_odl.username,
            cfg.CONF.ml2_odl.password,
            cfg.CONF.ml2_odl.timeout
        )
        self._context = neutron_context.get_admin_context()
        self._journal = journal
        self._task_flag = queue.LightQueue(maxsize=1)
        self._thread = None

    def intialize(self):
        self._thread = eventlet.greenthread.spawn(self._sync_state)

    def wakeup(self):
        try:
            self._task_flag.put_nowait(None)
        except queue.Full:
            pass

    def _wait(self):
        # NOTE(yamahata): timeout should be configurable? 1 sec for now
        try:
            self._task_flag.get(block=True, timeout=1)
        except queue.Empty:
            # timeout
            pass

    def _state_sync(self):
        while True:
            self._wait()
            self._process_journal()

    def _process_journal(self):
        while True:
            entry = self.journal.retrieve(self.context)
            if entry is None:
                return

            seqnum = entry['seqnum']
            try:
                self._sync_single_resource(entry)
            except Exception:
                # TODO(yamahata): state sync:
                # later check failed resource again backgroundly
                self.journal.commit_failed(seqnum)
            else:
                self.journal.committed(seqnum)
                self.journal.done(seqnum)

    def _sync_single_resource(self, entry):
        """Sync over a single resource from Neutron to OpenDaylight.

        Handle syncing a single operation over to OpenDaylight
        """
        # operation, object_type, context
        object_type = entry['resource_type']
        obj_id = entry['resource_id']
        operation = entry['operation']
        resource = entry['data']

        # TODO(yamahata): state sync: track synchronization state.
        # if dependent resources (e.g. network subnet in case of port)
        # synchronize dependent resources first

        # TODO(yamahata): optimize journal processing
        # check latest state of this resource in journal and use it
        # diredtly

        # Convert underscores to dashes in the URL for ODL
        object_type_url = object_type.replace('_', '-')
        try:
            if operation == odl_const.ODL_DELETE:
                # TODO(yamahata): deletion needs care.
                # Now 404 is ignored. It may cause stale resource in odl
                # due to rest request reorder.

                # TODO(yamahata): revise full sync, kick fullsync
                self.out_of_sync |= not self.client.try_delete(
                    object_type_url + '/' + obj_id)
            else:
                if operation == odl_const.ODL_CREATE:
                    urlpath = object_type_url
                    method = 'post'
                elif operation == odl_const.ODL_UPDATE:
                    urlpath = object_type_url + '/' + obj_id
                    method = 'put'
                self.client.sendjson(method, urlpath,
                                     {object_type_url[:-1]: resource})
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.error(_LE("Unable to perform %(operation)s on "
                              "%(object_type)s %(object_id)s"),
                          {'operation': operation,
                           'object_type': object_type,
                           'object_id': obj_id})
                # TODO(yamahata): revise full sync, kick fullsync
                self.out_of_sync = True

    ###########################################################################
    ###########################################################################
    ###########################################################################
    # TODO(yamahata): implement syncfull
    def sync_resources(self, plugin, dbcontext, collection_name):
        """Sync objects from Neutron over to OpenDaylight.

        This will handle syncing networks, subnets, and ports from Neutron to
        OpenDaylight. It also filters out the requisite items which are not
        valid for create API operations.
        """
        filter_cls = self.FILTER_MAP[collection_name]
        to_be_synced = []
        obj_getter = getattr(plugin, 'get_%s' % collection_name)
        if collection_name == odl_const.ODL_SGS:
            resources = obj_getter(dbcontext, default_sg=True)
        else:
            resources = obj_getter(dbcontext)
        for resource in resources:
            try:
                # Convert underscores to dashes in the URL for ODL
                collection_name_url = collection_name.replace('_', '-')
                urlpath = collection_name_url + '/' + resource['id']
                self.client.sendjson('get', urlpath, None)
            except requests.exceptions.HTTPError as e:
                with excutils.save_and_reraise_exception() as ctx:
                    if e.response.status_code == requests.codes.not_found:
                        filter_cls.filter_create_attributes_with_plugin(
                            resource, plugin, dbcontext)
                        to_be_synced.append(resource)
                        ctx.reraise = False
            else:
                # TODO(yamahata): compare result with resource.
                # If they don't match, update it below
                pass

        key = collection_name[:-1] if len(to_be_synced) == 1 else (
            collection_name)
        # Convert underscores to dashes in the URL for ODL
        collection_name_url = collection_name.replace('_', '-')
        self.client.sendjson('post', collection_name_url, {key: to_be_synced})

        # https://bugs.launchpad.net/networking-odl/+bug/1371115
        # TODO(yamahata): update resources with unsyned attributes
        # TODO(yamahata): find dangling ODL resouce that was deleted in
        # neutron db

    @utils.synchronized('odl-sync-full')
    def sync_full(self, plugin):
        """Resync the entire database to ODL.

        Transition to the in-sync state on success.
        Note: we only allow a single thread in here at a time.
        """
        if not self.out_of_sync:
            return
        dbcontext = neutron_context.get_admin_context()
        for collection_name in [odl_const.ODL_NETWORKS,
                                odl_const.ODL_SUBNETS,
                                odl_const.ODL_PORTS,
                                odl_const.ODL_SGS,
                                odl_const.ODL_SG_RULES]:
            self.sync_resources(plugin, dbcontext, collection_name)
        self.out_of_sync = False
