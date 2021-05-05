import os
import re
import numpy as np
import pandas as pd

class Combiner:

    def __init__(self, orig_file_pattern, file_encoding, data_separator,
        class_column_name, final_file_name):
        self.orig_file_pattern = orig_file_pattern
        self.file_encoding = file_encoding
        self.data_separator = data_separator
        self.class_column_name = class_column_name
        self.final_file_name = final_file_name
        
    
    def get_files(self):
        
        print(f'searching files with pattern: "{self.orig_file_pattern}"')
        matcher = re.compile(self.orig_file_pattern)
        
        orig_files = os.listdir("originals")
        work_files = []
        for f in orig_files:
            if matcher.match(f):
                print(f"found file: {f}")
                work_files.append(r"originals/" + f)
        
        self.org_files = work_files
    
    def join_files(self):
        pass
    
    def check_columns(self):
        pass

    def save_union_file(self):
        self.result_df.to_csv(r"unions/" + self.final_file_name + ".csv", index=False, sep= self.data_separator)
    
    def pipeline(self):
        self.get_files()
        self.join_files()
        self.check_columns()
        self.save_union_file()

        print("process complete")



class CombinerYYYYMM(Combiner):
    
    def __init__(self, orig_file_pattern, file_encoding, data_separator, class_column_name, final_file_name):
        super().__init__(orig_file_pattern, file_encoding, data_separator, class_column_name, final_file_name)
    
    def classify_rows(self, df):
        df[self.class_column_name] = df["col1"].str.replace("-","").str.slice(stop=6)
        return df

    def join_files(self):

        print('starting joining process...')

        df = pd.DataFrame()
        for i, f in enumerate(self.org_files):
            tmp_df = pd.read_csv(f, encoding = self.file_encoding, sep = self.data_separator)
            tmp_df = self.classify_rows(tmp_df)
            df = df.append(tmp_df, ignore_index=True)
            print(f"counting {i} - joining file: {f}")
            print(df.count())
        
        self.result_df = df

    def check_columns(self):

        col1 = []

        for f in self.org_files:
            tmp_df = pd.read_csv(f, encoding = self.file_encoding, sep = self.data_separator)
            # may seem a little weird, but it's just an example of how you can verify that the process went well
            col1.append(tmp_df.col1.str.slice(stop=2).astype('int32').sum())
            

        assert sum(col1) == self.result_df.col1.str.slice(stop=2).astype('int32').sum()
        

if __name__ == '__main__':
    # just give the: file name pattern, file encoding, data separator, 
    # classification column name, and name of the resulting file
    my_combiner = CombinerYYYYMM(r"ex\d_\d{4}.csv", "latin1", ";" , "DT_BASE", "res_file")
    # call the pipeline
    my_combiner.pipeline()

    