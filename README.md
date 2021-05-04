# This pipeline will happen in every case where we want to combine related CSV files in a single one. That is why it is going to be our [template](https://en.wikipedia.org/wiki/Template_method_pattern).
The steps are:
1. Define the name pattern of the files (so you can work with different sets of files at the same time)
2. If you want to classify the resulting rows in a way that you can determine the original file to which each row belongs (which is the main goal here), you must define the name of the column that you want to create to do that.
   - You should decide what data you will put in that column, for example, the year shown in the file's name or more specific information, like some data that results from some operation on other columns in each row.
3. You may want to check if the combination of all rows is right (kind of a paranoid mode).
4. Save the final table.

### The code shows an example of how to extend the main class to attend to a specific case like the following:

You want each row to be classified with a label of this kind: `yyyymm`, like in **201304**, so the class CombinerYYYYMM extends the class Combiner and creates a new method **classify_rows()** responsible for this behavior. This method is used inside a specialized **join_files()**.

This code depends on **NumPy** and **pandas**, if needed, run: `pip install -r requirements.txt`.
