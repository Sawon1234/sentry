from __future__ import absolute_import

import six

from .event_manager import default_manager


class Analytics(object):
    __all__ = ('record', 'validate')

    event_manager = default_manager

    def record(self, event_or_event_type, instance=None, **kwargs):
        """
        >>> record(Event())
        >>> record('organization.created', organization)
        """
        if isinstance(event_or_event_type, six.string_types):
            event = self.event_manager.get(
                event_or_event_type,
            ).from_instance(instance, **kwargs)
        else:
            event = event_or_event_type
        self.record_event(event)

    def record_event(self, event):
        """
        >>> record_event(Event())
        """

    def validate(self):
        """
        Validates the settings for this backend (i.e. such as proper connection
        info).

        Raise ``InvalidConfiguration`` if there is a configuration error.
        """
