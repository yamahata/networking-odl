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

from oslo_log import log as logging
from oslo_utils import importutils
from oslo_utils import uuidutils
from sqlalchemy.orm import exc as sql_exc

from neutron import context
from neutron.tests.unit import testlib_api

from networking_odl.common import constants
from networking_odl.db import journal_db


DB_PLUGIN_KLASS = 'neutron.db.db_base_plugin_v2.NeutronDbPluginV2'
LOG = logging.getLogger(__name__)
uuidgen = uuidutils.generate_uuid


class ODLJournalDbTestCase(testlib_api.SqlTestCase):
    """Unit test for ODL Journal DB."""

    def setUp(self):
        super(ODLJournalDbTestCase, self).setUp()
        self.ctx = context.get_admin_context()
        self.journal = journal_db.JournalDb()
        self.plugin = importutils.import_object(DB_PLUGIN_KLASS)
        self.journal.initialize(self.ctx)

    def _test_journal_log(self, entry_base):
        journal = self.journal
        seqnum = journal.log(self.ctx, entry_base['resource_type'],
                             entry_base['resource_id'],
                             entry_base['operation'], entry_base['resource'])
        entry_base['seqnum'] = seqnum
        entry_base['status'] = journal_db.STATUS_LOG
        entry = journal.peek(self.ctx, seqnum)
        self.assertEqual(entry_base, entry)
        return seqnum

    def _test_journal_retrieve(self, entry_base):
        entry = self.journal.retrieve(self.ctx)
        entry_base['status'] = journal_db.STATUS_COMMITTING
        self.assertEqual(entry_base, entry)

    def _test_journal_committed(self, seqnum, entry_base):
        journal = self.journal
        journal.committed(self.ctx, seqnum)
        entry = journal.peek(self.ctx, seqnum)
        entry_base['status'] = journal_db.STATUS_COMMITTED
        self.assertEqual(entry_base, entry)

    def _test_journal_commit_failed(self, seqnum, entry_base):
        journal = self.journal
        journal.commit_failed(self.ctx, seqnum)
        entry = journal.peek(self.ctx, seqnum)
        entry_base['status'] = journal_db.STATUS_COMMIT_FAILED
        self.assertEqual(entry_base, entry)

    def _test_journal_done(self, seqnum):
        journal = self.journal
        journal.done(self.ctx, seqnum)
        self.assertRaises(sql_exc.NoResultFound,
                          journal.peek, self.ctx, seqnum)

        # now entry of seqnum is deleted.
        self.assertRaises(sql_exc.NoResultFound, journal.committed,
                          self.ctx, seqnum)
        self.assertRaises(sql_exc.NoResultFound, journal.done,
                          self.ctx, seqnum)

    # TODO(yamahata): This test can't be run parallelly
    def test_journal_entry(self):
        resource_type = constants.ODL_NETWORK
        operation = constants.ODL_CREATE

        # test with only one entry
        resource_id = uuidgen()
        resource = "{'dammy-key': 'dammy-resource'}"
        entry_base = {'resource_type': resource_type,
                      'resource_id': resource_id,
                      'operation': operation,
                      'resource': resource}

        seqnum = self._test_journal_log(entry_base)
        self._test_journal_retrieve(entry_base)
        self._test_journal_committed(seqnum, entry_base)
        self._test_journal_done(seqnum)

        # test with two entries
        resource_id0 = uuidgen()
        resource_id1 = uuidgen()
        resource0 = "{'dammy-key0': 'dammy-resource0'}"
        resource1 = "{'dammy-key1': 'dammy-resource1'}"
        entry_base0 = {'resource_type': resource_type,
                       'resource_id': resource_id0,
                       'operation': operation,
                       'resource': resource0}
        entry_base1 = {'resource_type': resource_type,
                       'resource_id': resource_id1,
                       'operation': operation,
                       'resource': resource1}

        seqnum0 = self._test_journal_log(entry_base0)
        seqnum1 = self._test_journal_log(entry_base1)
        self.assertLess(seqnum, seqnum0)
        self.assertLess(seqnum0, seqnum1)
        self._test_journal_retrieve(entry_base0)
        self._test_journal_committed(seqnum0, entry_base0)
        self._test_journal_done(seqnum0)
        self._test_journal_retrieve(entry_base1)
        self._test_journal_committed(seqnum1, entry_base1)
        self._test_journal_done(seqnum1)

        # test with only one entry to fail
        resource_id = uuidgen()
        resource = "{'dammy-key': 'dammy-resource'}"
        entry_base = {'resource_type': resource_type,
                      'resource_id': resource_id,
                      'operation': operation,
                      'resource': resource}

        seqnum = self._test_journal_log(entry_base)
        self._test_journal_retrieve(entry_base)
        self._test_journal_commit_failed(seqnum, entry_base)
