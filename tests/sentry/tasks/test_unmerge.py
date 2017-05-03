from __future__ import absolute_import

import itertools
import uuid

from collections import OrderedDict

from sentry.models import Event, Group, GroupHash
from sentry.testutils import TestCase
from sentry.tasks.unmerge import get_fingerprint, unmerge


class UnmergeTestCase(TestCase):
    def test_unmerge(self):
        project = self.create_project()
        source = self.create_group(project)

        sequence = itertools.count(0)

        def create_message_event(template, parameters):
            return Event.objects.create(
                project_id=project.id,
                group_id=source.id,
                event_id=uuid.UUID(
                    fields=(
                        next(sequence),
                        0x0,
                        0x1000,
                        0x80,
                        0x80,
                        0x808080808080,
                    ),
                ).hex,
                message='%s' % (id,),
                data={
                    'type': 'default',
                    'metadata': {
                        'title': template % parameters,
                    },
                    'sentry.interfaces.Message': {
                        'message': template,
                        'params': parameters,
                        'formatted': template % parameters,
                    },
                },
            )

        events = OrderedDict()

        for event in (create_message_event('This is message #%s.', i) for i in xrange(10)):
            events.setdefault(get_fingerprint(event), []).append(event)

        for event in (create_message_event('This is message #%s!', i) for i in xrange(10, 20)):
            events.setdefault(get_fingerprint(event), []).append(event)

        assert len(events) == 2
        assert sum(map(len, events.values())) == 20

        # XXX: This is super contrived considering that it doesn't actually go
        # through the event pipeline, but them's the breaks, eh?
        for fingerprint in events.keys():
            GroupHash.objects.create(
                project=project,
                group=source,
                hash=fingerprint,
            )

        # TODO: Event mappings
        # TODO: User reports

        destination = Group.objects.get(
            id=unmerge(
                source.id,
                [events.keys()[1]],
                None,
                batch_size=5,
            ),
        )

        assert source.id != destination.id
        assert source.project == destination.project

        assert source.event_set.count() == 10
        assert source.grouphash_set.count() == 1
        assert source.grouphash_set.filter(hash=events.keys()[0]).count() == 1

        # TODO: Validate attributes

        assert destination.event_set.count() == 10
        assert destination.grouphash_set.count() == 1
        assert destination.grouphash_set.filter(hash=events.keys()[1]).count() == 1

        # TODO: Validate attributes
