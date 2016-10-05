#!/bin/bash

if [ ${compwa_app_path} ] && [ ${output_dir} ] && [ ${output_file} ] && [ ${input_dir} ] && [ ${input_file_data} ] && [ ${input_file_data} ] && [ ${PBS_ARRAYID} ]; then
  if [ ${use_local_file} ]; then
    real_output_dir=${output_dir}
    output_dir="$(mktemp -d -p /local/scratch)"
  fi
  
  #add the array id as a suffix to input file
  #fullfilename=$(basename "${input_file_data}")
  extension="${input_file_data##*.}"
  filename="${input_file_data%.*}"
  input_file_data="${filename}_${PBS_ARRAYID}.${extension}"

  random_seed=$RANDOM
  echo "Running ${compwa_app_path} ${input_dir}/${input_file_data} ${input_dir}/${input_file_phsp_data} ${output_dir} ${output_file} ${random_seed} ${PBS_ARRAYID}"
  ${compwa_app_path} "${input_dir}/${input_file_data}" "${input_dir}/${input_file_phsp_data}" ${output_dir} ${output_file} ${random_seed} ${PBS_ARRAYID}
  
  if [ ${use_local_file} ]; then
    mv ${output_dir}/* ${real_output_dir}/.
    rm -rf ${output_dir}
  fi
fi

exit 0
