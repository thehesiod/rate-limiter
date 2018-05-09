import asynctest
import unittest

_none_type = type(None)


def _assertRecursiveAlmostEqual(self, first, second, places=None, msg=None, delta=None):
    """Fail if the two objects are unequal as determined by their
       difference rounded to the given number of decimal places
       (default 7) and comparing to zero, or by comparing that the
       between the two objects is more than the given delta.

       Note that decimal places (from zero) are usually not the same
       as significant digits (measured from the most signficant digit).

       If the two objects compare equal then they will automatically
       compare almost equal.
    """

    if type(first) != type(second) and not (isinstance(first, (float, int, complex)) and isinstance(second, (float, int, complex))):
        return self.assertEqual(first, second)  # will raise mis-matched types

    if isinstance(first, (_none_type, str)):
        self.assertEqual(first, second)
    elif isinstance(first, (float, int, complex)):
        self.assertAlmostEqual(first, second, places, msg, delta)
    elif isinstance(first, dict):
        self.assertEqual(set(first.keys()), set(second.keys()))  # will raise keys don't match

        for f_k, f_v in first.items():
            try:
                self.assertRecursiveAlmostEqual(f_v, second[f_k], places, msg, delta)
            except Exception as e:
                raise Exception("Error with key: {}".format(f_k)) from e
    elif isinstance(first, (list, tuple)):
        if len(first) != len(second):
            self.assertEqual(first, second)  # will raise list don't have same length

        for idx in range(len(first)):
            try:
                self.assertRecursiveAlmostEqual(first[idx], second[idx], places, msg, delta)
            except Exception as e:
                raise Exception("Error with index: {}".format(idx)) from e
    else:
        assert False  # unsupported


# Monkeypatch in method
asynctest.TestCase.assertRecursiveAlmostEqual = _assertRecursiveAlmostEqual
unittest.TestCase.assertRecursiveAlmostEqual = _assertRecursiveAlmostEqual
