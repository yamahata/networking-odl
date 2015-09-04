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

from oslo_db import api as oslo_db_api
from oslo_log import log as logging
import sqlalchemy as sa
from sqlalchemy.orm import exc

from neutron.db import api as db_api
from neutron.db import common_db_mixin
from neutron.db import model_base


LOG = logging.getLogger(__name__)

STATUS_LOG = 'LOG'
STATUS_COMMITTING = 'COMMITTING'
STATUS_COMMITTED = 'COMMITTED'
STATUS_COMMIT_FAILED = 'COMMIT_FAILED'
STATUS_KEEP_FOR_SEQNUM = 'KEEP_FOR_SEQNUM'


class ODLJournal(model_base.BASEV2):
    """Represents a ODL journal."""

    __tablename__ = "odl_journal"

    seqnum = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    resource_type = sa.Column(sa.String(255), nullable=True)
    resource_id = sa.Column(sa.String(36), nullable=False)
    operation = sa.Column(sa.String(255), nullable=False)
    resource = sa.Column(sa.String(4096), nullable=False)
    # LOG, COMMITING, COMMITED,
    status = sa.Column(sa.String(16), nullable=False)


class JournalDb(common_db_mixin.CommonDbMixin):
    @staticmethod
    def _make_journal_dict(journal):
        return {'seqnum': journal['seqnum'],
                'resource_type': journal['resource_type'],
                'resource_id': journal['resource_id'],
                'operation': journal['operation'],
                'resource': journal['resource'],
                'status': journal['status']}

    def __init__(self):
        super(JournalDb, self).__init__()

    def initialize(self, context):
        pass

    def peek(self, context, seqnum):
        """helper method for test"""
        return self._make_journal_dict(
            context.session.query(ODLJournal).
            filter(ODLJournal.resource_type.isnot(None),
                   ODLJournal.seqnum == seqnum).
            one())

    def log(self, context, resource_type, resource_id, operation, resource):
        assert resource_type is not None
        session = context.session
        with session.begin(subtransactions=True):
            journal = ODLJournal(
                resource_type=resource_type,
                resource_id=resource_id,
                operation=operation,
                resource=resource,
                status=STATUS_LOG)
            session.add(journal)
        return journal.seqnum

    @oslo_db_api.wrap_db_retry(max_retries=db_api.MAX_RETRIES,
                               retry_on_request=True,
                               retry_on_deadlock=True)
    def retrieve(self, context):
        session = context.session
        retry = db_api.MAX_RETRIES
        while retry > 0:
            retry -= 1
            with session.begin(subtransactions=True):
                # select * from journal order by seqnum limit 1
                journal = (session.query(ODLJournal).
                           filter_by(status=STATUS_LOG).
                           order_by(ODLJournal.seqnum).first())
            if journal is None:
                return None
            retval = self._make_journal_dict(journal)
            with session.begin(subtransactions=True):
                count = (session.query(ODLJournal).
                         filter_by(seqnum=retval['seqnum'], status=STATUS_LOG).
                         update({'status': STATUS_COMMITTING}))
            if count == 0:
                continue

            retval['status'] = STATUS_COMMITTING
            return retval

    def committed(self, context, seqnum):
        session = context.session
        with session.begin(subtransactions=True):
            count = (session.query(ODLJournal).
                     filter_by(seqnum=seqnum, status=STATUS_COMMITTING).
                     update({'status': STATUS_COMMITTED}))
        if count == 0:
            raise exc.NoResultFound()

    def commit_failed(self, context, seqnum):
        session = context.session
        with session.begin(subtransactions=True):
            count = (session.query(ODLJournal).
                     filter_by(seqnum=seqnum, status=STATUS_COMMITTING).
                     update({'status': STATUS_COMMIT_FAILED}))
        if count == 0:
            raise exc.NoResultFound()

    def done(self, context, seqnum):
        session = context.session
        with session.begin(subtransactions=True):
            count = (session.query(ODLJournal).
                     filter_by(seqnum=seqnum, status=STATUS_COMMITTED).
                     update({'resource_type': None,
                             'status': STATUS_KEEP_FOR_SEQNUM}))
            if count == 0:
                raise exc.NoResultFound()

            (session.query(ODLJournal).
             filter(ODLJournal.seqnum < seqnum,
                    ODLJournal.status == STATUS_KEEP_FOR_SEQNUM).
             delete())
