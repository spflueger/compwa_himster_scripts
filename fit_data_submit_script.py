#!/usr/bin/python

import himster_functions
import argparse, os

himster_total_job_threshold = 1500

parser = argparse.ArgumentParser(description='Script for running ComPWA applications as job arrays on HIMSTER.', formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('application_path', metavar='application_path', type=str, nargs=1, 
                    help='Path of the application that will be submitted and run on HIMSTER. Absolute paths are safer but relative are also possible.')
parser.add_argument('--input_directory', metavar='input_directory', type=str, default=os.getcwd(),
                    help='Input directory for the data files.')
parser.add_argument('--input_filename', metavar='input_filename', type=str, default='data.root',
                    help='Input data file name template (signal data). The job index will be added as a suffix!')
parser.add_argument('--input_filename_phsp', metavar='input_filename_phsp', type=str, default='3Part-4vecs.root',
                    help='Input phase space data file name for the normalization.')
parser.add_argument('--output_directory', metavar='output_directory', type=str, default='',
                    help='Output directory for the fit results etc. Default is same as input directory')
parser.add_argument('--output_filename', metavar='output_filename', type=str, default='FitResult.root',
                    help='Output file name for the main fit result.')
parser.add_argument('--logfile_output_directory', metavar='logfile_output_directory', type=str, default='',
                    help='Output directory for the log files. Default is same directory as for the data file output.')
parser.add_argument('--low_index', metavar='low_index', type=int, default= 1,
                   help='Lowest index of the job array to be run. Default is just a single job, so 1')
parser.add_argument('--high_index', metavar='high_index', type=int, default= 1,
                   help='Highest index of the job array to be run. Default is just a single job, so 1')

args = parser.parse_args()

workdir = os.getcwd()
input_dir = args.input_directory
if args.output_directory != '' and os.path.isdir(args.output_directory):
  output_dir = args.output_directory
else:
  output_dir = input_dir
if args.logfile_output_directory != '' and os.path.isdir(args.logfile_output_directory):
  logfile_outdir = args.logfile_output_directory
else:
  logfile_outdir = output_dir

application_name = os.path.basename(str(args.application_path[0]))
walltime='12:00:00'

cluster_jobs=himster_functions.ClusterJobs('runFitApplication.sh', application_name, logfile_outdir, walltime)
cluster_jobs.job_range_low = args.low_index
cluster_jobs.job_range_high = args.high_index

cluster_jobs.insertJobVariable('compwa_app_path', args.application_path[0])
cluster_jobs.insertJobVariable('output_dir', output_dir)
cluster_jobs.insertJobVariable('output_file', args.output_filename)
cluster_jobs.insertJobVariable('input_dir', input_dir)
cluster_jobs.insertJobVariable('input_file_data', args.input_filename)
cluster_jobs.insertJobVariable('input_file_phsp_data', args.input_filename_phsp)

himster_functions.submitJobsToCluster(cluster_jobs)
