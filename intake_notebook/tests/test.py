import intake
import glob
books =glob.glob('C:/data/providance/experiment/*.ipynb')
cat =intake.open_intake_notebook(books)
cat.read()