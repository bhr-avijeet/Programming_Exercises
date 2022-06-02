import unittest
from BankAccount import *


class BankAccountUnitTest(unittest.TestCase):

    def test_withdraw_funds(self):
        b1 = BankAccount("Test", 100)
        res = b1.withdraw_funds(50)
        self.assertEqual(res, 50)

    # def test_exception(self):
    #     b1 = BankAccount("Test", 100)
    #     with self.assertRaises(Exception) as context:
    #         b1.withdraw_funds(101)
    #
    #     print(context.exception)


if __name__ == '__main__':
    unittest.main()
