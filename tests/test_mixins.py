import unittest
from pysetl.mixins import CanCreateMixin

class Test_Mixins(unittest.TestCase):
    def test_CanCreateMixin(self):
        class ObjectWithCanCreate(CanCreateMixin): pass

        can_create = ObjectWithCanCreate()

        return hasattr(can_create, "create") and callable(getattr(can_create, "create"))
        
if __name__ == '__main__':
    unittest.main()