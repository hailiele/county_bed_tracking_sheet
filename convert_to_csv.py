
import os
import fnmatch
import pandas as pd

def set_dir():
	os.chdir(os.getcwd())
	return os.getcwd()

def convert_to_csv(wd=""):
	for file in os.listdir(wd):
		if file.endswith(".xlsx"):
			read_file = pd.read_excel(file)
			csv_name = os.path.splitext(file)[0] + ".csv"
			read_file.to_csv (csv_name, index = None, header=True)

def main():
	wd = set_dir()
	convert_to_csv(wd)


if __name__=="__main__":
	main()
