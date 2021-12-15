import unittest
from pysetl.mixins import CanCreateMixin, CanDeleteMixin, CanDropMixin


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


if __name__ == '__main__':
    unittest.main()
