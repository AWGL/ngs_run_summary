# NGS_run_summary

## Download the directory:
```
git clone git@github.com:AWGL/ngs_run_summary.git
```

## Create and activate the conda environment
```
conda env create -f ngs_run_summary.yml
conda activate ngs_run_summary
```

## Requirements

Requires:
* Access to NGS output folder which contains the run folders
Each run folder should have:
* "InterOp" folder
* "RunParameters.xml" file
* "SampleSheet.csv" file


## To run the programme

To view help:
```
python ngs_run_summary.py -h
```

The programme will default to last months start and end date:
```
python ngs_run_summary.py
```

 or an optional start and end date can be specified in the format YYMMDD:
```
python ngs_run_summary.py -s 210101 -e 210131
```

## Additional info

*Code is set up to check for some logic problems with input dates and will report a STDOUT
*Output is a csv file in the current directory
*Code will also STDOUT the start and end dates, along with a count of runs found within the range
*If no runs are found within date range then a blank csv is output and a run count of 0 is displayed in STDOUT
