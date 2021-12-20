import unittest
from pysetl.mixins import (
    CanCreateMixin,
    CanDeleteMixin,
    CanDropMixin,
    CanPartitionMixin,
    CanUpdateMixin,
    CanVaccumMixin,
    CanWaitMixin
)


class Test_Mixins(unittest.TestCase):
    def test_CanCreateMixin(self):
        class ObjectWithCanCreate(CanCreateMixin):
            pass

        can_create = ObjectWithCanCreate()

        return(
            hasattr(can_create, "create") and
            callable(getattr(can_create, "create"))
        )

    def test_CanDeleteMixin(self):
        class ObjectWithCanDelete(CanDeleteMixin):
            pass

        can_delete = ObjectWithCanDelete()

        return(
            hasattr(can_delete, "delete") and
            callable(getattr(can_delete, "delete"))
        )

    def test_CanDropMixin(self):
        class ObjectiWithCanDrop(CanDropMixin):
            pass

        can_delete = ObjectiWithCanDrop()

        return(
            hasattr(can_delete, "delete") and
            callable(getattr(can_delete, "delete"))
        )

    def test_CanPartitionMixin(self):
        class ObjectiWithCanPartition(CanPartitionMixin):
            pass

        can_delete = ObjectiWithCanPartition()

        return(
            hasattr(can_delete, "drop") and
            callable(getattr(can_delete, "drop"))
        )

    def test_CanUpdateMixin(self):
        class ObjectiWithCanUpdate(CanUpdateMixin):
            pass

        can_delete = ObjectiWithCanUpdate()

        return(
            hasattr(can_delete, "update") and
            callable(getattr(can_delete, "update"))
        )

    def test_CanVaccumMixin(self):
        class ObjectWithCanVaccum(CanVaccumMixin):
            pass

        can_vaccum = ObjectWithCanVaccum()

        return(
            hasattr(can_vaccum, "vacum") and
            callable(getattr(can_vaccum, "vaccum"))
        )

    def test_CanWaitMixin(self):
        class ObjectWithCanWait(CanWaitMixin):
            pass

        can_wait = ObjectWithCanWait()

        return(
            hasattr(can_wait, "await_termination") and
            callable(getattr(can_wait, "await_termination"))
        )


if __name__ == '__main__':
    unittest.main()
