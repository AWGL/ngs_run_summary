import os
import datetime
import sys
from interop import imaging, summary
import pandas as pd
import xmltodict



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
	check if input variables were given, else get default as "last month"
	if only one date value given then will still default to last month as start and end dates
are over written
'''
#	arg dates must be YYMMDD
try:
	startDate = sys.argv[1]
	endDate = sys.argv[2]

except IndexError:
	print ("Invalid/No dates inputted. Defaulted to last month")
	today = datetime.date.today()
	firstday = today.replace(day=1)
	prevMonthEnd = firstday - datetime.timedelta(days=1)
	prevMonthStart = prevMonthEnd.replace(day=1)
	startDate = prevMonthStart.strftime("%y%m%d")
	endDate = prevMonthEnd.strftime("%y%m%d")

print("Start: ",startDate)
print("End: ",endDate)

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

path = "/home/kal/ngs_run_summary/data/"

#	loop through all folders in the data folder
dir_list = os.listdir(path)
run_counter = 0
for folder in dir_list:
	if int(startDate) <= int(folder[:6]) <= int(endDate):
		run_counter +=1
		run_folder = os.path.join(path,folder)

		interop_out = get_interops_data(run_folder)
		run_summary = interop_out

		parameters_out = get_run_parameters(run_folder)
		run_summary.extend(parameters_out)

		summarysheet_out = get_pipeline_name(run_folder)
		run_summary.extend(summarysheet_out)

		ngs_summary_df.loc[len(ngs_summary_df)] = run_summary


run_count = "There were " + str(run_counter) + " runs between " + startDate + " and " + endDate
print(run_count)

#	output to csv
ngs_summary_df.to_csv('ngs_run_summary_'+startDate+'_'+endDate+'.csv', index = False)
