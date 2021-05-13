import os
import datetime
import sys
from interop import imaging, summary
import pandas as pd
import xmltodict
import argparse



def default_args(start_arg,end_arg):
	'''
	take supplied arguments and if none given will have defaulted to "last_month" which is used to 
	give a start and end date relating to last month as automatic
	'''
	if start_arg == "last_month":
		todaydate = datetime.date.today()
		firstday = todaydate.replace(day=1)
		prev_month_end = firstday - datetime.timedelta(days=1)
		prev_month_start = prev_month_end.replace(day=1)
		start_date = prev_month_start.strftime("%y%m%d")
	else:
		start_date = start_arg

	if end_arg == "last_month":
		todaydate = datetime.date.today()
		firstday = todaydate.replace(day=1)
		prev_month_end = firstday - datetime.timedelta(days=1)
		end_date = prev_month_end.strftime("%y%m%d")
	else:
		end_date = end_arg

	return start_date, end_date



def check_args(start,end):
	'''
	 Logic check for inputs if not defaulted. Must be 6 characters long, end must be after first,
	middle 2 characters must be between 01 and 12, end two characters must be between 01 and 31,
	'''
	# check start
	if len(start) == 6:
		if 1 <= int(start[2:4]) <= 12:
			if 1 <= int(start[4:6]) <= 31:
				start_test = True
			else:
				start_test = False
		else:
			start_test = False
	else:
		start_test = False


	# check end
	if len(end) == 6:
		if 1 <= int(end[2:4]) <= 12:
			if 1 <= int(end[4:6]) <= 31:
				end_test = True
			else:
				end_test = False
		else:
			end_test = False
	else:
		end_test = False


	# check both tests are True and end is >= start
	if start_test == True and end_test == True:
		if end >= start:
			continue_run = True
			end_ge_start = "NA"
		else:
			end_ge_start = False
			continue_run = False
	else:
		end_ge_start = "NA"
		continue_run = False

	# handy STDOUT
	print("Input checks- start:", start_test, ", end:", end_test, ", start >= end:", end_ge_start)

	return continue_run


def get_interops_data(run_folder_path):
	'''
	Function to retrieve interops data from run folder

	input a path to run folder eg: "/home/kal/ngs_run_summary/data/201209_A00748_0065_AHT3CJDMXX"

	values are returned as a string:
	run id, yield, %>Q30, %passfilter
	'''
	
	run_id = str(os.path.basename(run_folder_path))

	#	create dataframe from interops summary option, and pull yield and %>Q30
	summary_ar = summary(run_folder_path)
	summary_df = pd.DataFrame(summary_ar)
	summary_yield = summary_df.at[0,"Yield G"]
	summary_Q30 = summary_df.at[0,"% >= Q30"]

	#	create dataframe from interops run metrics option, average all rows, and pull %passfilter
	metric_ar = imaging(run_folder_path)
	metric_df = pd.DataFrame(metric_ar)
	av_column = metric_df.mean(axis=0)
	av_passfilter = av_column["% Pass Filter"]

	interop_data_out = [run_id,str(summary_yield),str(summary_Q30),str(av_passfilter)]

	return interop_data_out



def get_run_parameters(run_folder_path):
	'''
	Function to retrieve run parameter information from runparameters.xml

	input a path to a run folder

	values are returned as a string:
	experiment name,seq side, flowcell type
	'''
	parameters_filepath = os.path.join(run_folder_path,"RunParameters.xml")

	with open(parameters_filepath) as f:
		run_paramaters_dict = xmltodict.parse(f.read())

	side = run_paramaters_dict["RunParameters"]["Side"]
	exp_name = run_paramaters_dict["RunParameters"]["ExperimentName"]
	flowcell_type = run_paramaters_dict["RunParameters"]["RfidsInfo"]["FlowCellMode"]

	run_param_out = [exp_name,side,flowcell_type]

	return run_param_out


def get_pipeline_name(run_folder_path):
	'''
	Function to retrieve pipeline name(s) from the samplesheet for the run.

	input a path to a run folder

	pipeline names are returned as a single item list, in the event that there is no "description" column
	in the samplesheet then list is returned as ["No data"]. Multiple pipelines will exist within one
	item on the output list so appending to ngs_summary won't add multiple cells.
	'''
	samplesheet_path = os.path.join(run_folder_path,"SampleSheet.csv")

	#	get row marker for the data sub-table
	#	get row marker for the data sub-table
	data = pd.read_csv(samplesheet_path)
	counter = 0 # excel counts from 1
	for row in data.iloc[:,0]:
		if row != "[Data]":
			counter+= 1
		else:
			row_marker = counter +1

	#	get column marker for description in data sub-table
	sample_df_cols = list(data.iloc[row_marker,:])
	counter = 0
	for col in sample_df_cols:
		if col != "Description":
			counter +=1
		else:
			col_marker = counter

	#	create list of different pipelines, then convert to string
	#	create desc_list of description column if found, else "None"
	#	will not populate pipeline_names without finding "pipelineName"
	try:
		desc_list = list(data.iloc[row_marker+1:,col_marker])
	except NameError:
		desc_list = ["None"]

	pipeline_names = []
	for item in desc_list:
		for piece in item.split(";"):
			if piece[:12] == "pipelineName":
				pipeline = piece[13:]
				if pipeline not in pipeline_names:
					pipeline_names.append(pipeline)

	#	if markers are not found then nothing appended to pipeline_names so "no data"
	if len(pipeline_names) == 0:
		pipeline_list = ["No data"]
	elif len(pipeline_names)>1:
		pipeline_list = [" & ".join(pipeline_names)]
	else:
		pipeline_list = pipeline_names

	return pipeline_list




'''
############################ PROGRAMME CODE ##################################
'''

# args
parser = argparse.ArgumentParser()
parser.add_argument("--startdate","-s", default = "last_month", help = "Start date for range of NGS runs. Inclusive. Default: last month. Format: YYMMDD",)
parser.add_argument("--enddate","-e", default = "last_month", help = "End date for range of NGS runs. Inclusive. Default: last month. Format: YYMMDD")
args = parser.parse_args()

# check for start and end date inputs and make default if "last_month"(no input)
start_date, end_date = default_args(args.startdate,args.enddate)
print("start date:", start_date, ", end date:", end_date)


#check date is as expected YYMMDD
continue_run = check_args(start_date, end_date)

#start run only if check_args = True
if continue_run == True:
	print("Input arguments checked successfully")

	'''
		Find run folders in data directory and loop over those which are within the date window specific by 
	start and end dates
		Create a counter so in the event of no runs in the time period it will just return "there was 0 runs"
	and would not look like a code failure
		Append summary data for each run to a dataframe to export
	'''
	#	create blank DF
	col_headers = ["Run_ID","Yield_g","Percent_gt_Q30","Percent_pass_filter","Experiment_name","Sequencer_side","Flowcell_type","Pipeline(s)"]
	ngs_summary_df = pd.DataFrame(columns = col_headers)

	path = "/data_heath/archive/novaseq/"

	#	loop through all folders in the data folder
	dir_list = os.listdir(path)
	run_counter = 0
	for folder in dir_list:
		if int(start_date) <= int(folder[:6]) <= int(end_date):
			run_counter +=1
			run_folder = os.path.join(path,folder)

			interop_out = get_interops_data(run_folder)
			run_summary = interop_out

			parameters_out = get_run_parameters(run_folder)
			run_summary.extend(parameters_out)

			summarysheet_out = get_pipeline_name(run_folder)
			run_summary.extend(summarysheet_out)

			ngs_summary_df.loc[len(ngs_summary_df)] = run_summary


	run_count = f"There were {run_counter} runs between  {start_date} and {end_date}"
	print(run_count)

	#output to csv
	ngs_summary_df.to_csv('ngs_run_summary_'+start_date+'_'+end_date+'.csv', index = False)

else:
	print("Run failed, check input arguments are logical and in the format YYMMDD")
