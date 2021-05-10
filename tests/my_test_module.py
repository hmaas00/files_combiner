import unittest

import pandas as pd
from combiner import Combiner, CombinerYYYYMM

class TestMethods(unittest.TestCase):

    def test_get_files(self):
        my_combiner = CombinerYYYYMM(r"ex\d_\d{4}.csv", "latin1", ";" , "DT_BASE", "res_file")
        my_combiner.get_files()
        self.assertTrue(len(my_combiner.org_files) > 0)

    def test_join_files(self):
        my_combiner = CombinerYYYYMM(r"ex\d_\d{4}.csv", "latin1", ";" , "DT_BASE", "res_file")
        my_combiner.get_files()
        my_combiner.join_files()
        self.assertTrue(isinstance(my_combiner.result_df, pd.DataFrame))


# from root run:
# python -m unittest tests\my_test_module.py