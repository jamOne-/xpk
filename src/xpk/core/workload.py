"""
Copyright 2024 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import re
from ..utils.console import xpk_exit, xpk_print
from .commands import run_command_for_value
from .gcloud_context import get_cluster_location

WORKLOAD_LIST_DELIMITER = '~'


def parse_and_format_workload_list(
    raw_output: str, filter_by_status: str, filter_by_job: str
) -> str:
  """Parses, filters, and formats the raw workload list output.

  Args:
    raw_output: The raw output from kubectl.
    filter_by_status: The status filter to apply.
    filter_by_job: The job name filter to apply.

  Returns:
    The formatted table string.
  """
  headers = [
      'Jobset Name',
      'Created Time',
      'Priority',
      'TPU VMs Needed',
      'TPU VMs Running/Ran',
      'TPU VMs Done',
      'Status',
      'Status Message',
      'Status Time',
  ]
  rows = []
  if raw_output:
    for line in raw_output.splitlines():
      parts = line.split(WORKLOAD_LIST_DELIMITER)
      if len(parts) != len(headers):
        # Skip malformed lines or handle error
        continue

      # Sum up count fields (indices 3, 4, 5)
      def sum_counts(field_val):
        if not field_val:
          return 0
        try:
          return sum(int(x) for x in field_val.split())
        except ValueError:
          return 0

      # Create a mutable list for the row
      row = list(parts)

      # Process counts
      needed = sum_counts(parts[3])
      running = sum_counts(parts[4])
      done = sum_counts(parts[5])

      row[3] = str(needed)
      row[4] = str(running) if running > 0 else '<none>'
      row[5] = str(done) if done > 0 else '<none>'

      # Filter Logic
      status = row[6]
      message = row[7]

      include = False
      if filter_by_status == 'EVERYTHING':
        include = True
      elif filter_by_status == 'RUNNING':
        # Status ~ "Admitted|Evicted" && Running > 0
        if status in ['Admitted', 'Evicted'] and running > 0:
          include = True
      elif filter_by_status == 'QUEUED':
        # Status ~ "Admitted|Evicted|QuotaReserved" && Running == 0
        # (Original logic checked for <none> or 0)
        if status in ['Admitted', 'Evicted', 'QuotaReserved'] and running == 0:
          include = True
      elif filter_by_status == 'FINISHED':
        if status == 'Finished':
          include = True
      elif filter_by_status == 'FAILED':
        if status == 'Finished' and 'failed' in message:
          include = True
      elif filter_by_status == 'SUCCESSFUL':
        if status == 'Finished' and 'finished' in message:
          include = True
      else:
        raise RuntimeError(f'Can not find filter type: {filter_by_status}')

      if include and filter_by_job:
        if filter_by_job not in row[0]:
          include = False

      if include:
        rows.append(row)

  # Formatting
  if not rows:
    return ''

  # Calculate column widths
  col_widths = [len(h) for h in headers]
  for row in rows:
    for i, val in enumerate(row):
      col_widths[i] = max(col_widths[i], len(val))

  # Create format string
  fmt = '   '.join(f'{{:<{w}}}' for w in col_widths)

  output = [fmt.format(*headers)]
  for row in rows:
    output.append(fmt.format(*row))

  return '\n'.join(output)


def get_workload_list(args) -> tuple[int, str]:
  """Function to get the list of the workloads in the cluster.

  Args:
    args: user provided arguments for running the command.

  Returns:
    return_code: 0 if successful and 1 otherwise.
    return_value: workloads in the cluster matching the criteria.
  """
  # Define JSONPath with delimiter
  jsonpath_parts = [
      '{.metadata.ownerReferences[0].name}',
      '{.metadata.creationTimestamp}',
      '{.spec.podSets[0].template.spec.priorityClassName}',
      '{.spec.podSets[*].count}',
      '{.status.admission.podSetAssignments[*].count}',
      '{.status.reclaimablePods[*].count}',
      '{.status.conditions[-1].type}',
      '{.status.conditions[-1].message}',
      '{.status.conditions[-1].lastTransitionTime}',
  ]
  # Use range to iterate over items and print delimiter-separated values
  # {range .items[*]}part1~part2~...partN{"\n"}{end}

  # Note: Status Message might contain newlines, which could break parsing.
  # However, kubectl jsonpath output is raw. We rely on the delimiter.
  # If a message contains the delimiter, we are in trouble.
  # But ~ is rare in k8s messages.

  delimiter = WORKLOAD_LIST_DELIMITER
  jsonpath_str = (
      f'{{range .items[*]}}{delimiter.join(jsonpath_parts)}{{"\n"}}{{end}}'
  )

  # Escape newline for shell
  jsonpath_str = jsonpath_str.replace('\n', '\\n')

  command = (
      f"kubectl get workloads --ignore-not-found -o=jsonpath='{jsonpath_str}'"
  )

  task = f'List Jobs with filter-by-status={args.filter_by_status}'
  if hasattr(args, 'filter_by_job') and args.filter_by_job:
    task += f' with filter-by-job={args.filter_by_job}'

  return_code, return_value = run_command_for_value(command, task)

  if return_code != 0:
    return return_code, return_value

  formatted_output = parse_and_format_workload_list(
      return_value, args.filter_by_status, getattr(args, 'filter_by_job', None)
  )

  return 0, formatted_output


def check_if_workload_exists(args) -> bool:
  """Check if workload exists.

  Args:
     args: user provided arguments for running the command.

  Returns:
    returns true if workload exist, otherwise returns false.
  """
  columns = {
      'Jobset': '.metadata.ownerReferences[0].name',
  }

  s = ','.join([key + ':' + value for key, value in columns.items()])

  command = f"kubectl get workloads -o=custom-columns='{s}'"
  return_code, return_msg = run_command_for_value(
      command, 'Check if Workload Already Exists'
  )

  if return_code != 0:
    xpk_print(f'List Job request returned ERROR {return_code}')
    xpk_exit(return_code)

  lines = return_msg.split('\n')
  new_workload_name = args.workload
  for line in lines:
    if line == new_workload_name:
      return True
  return False


def wait_for_job_completion(args) -> int:
  """Function to wait for job completion.

  Args:
    args: user provided arguments for running the command.

  Returns:
    return_code: 0 if successful, 124 if timeout, 125 if unsuccessful job, 1 otherwise
  """
  # Check that the workload exists
  args.workload = args.wait_for_job_completion
  workload_exists = check_if_workload_exists(args)
  if not workload_exists:
    xpk_print(f'Workload named {args.workload} does not exist.')
    return 1

  # Get the full workload name
  get_workload_name_cmd = f'kubectl get workloads | grep jobset-{args.workload}'
  return_code, return_value = run_command_for_value(
      get_workload_name_cmd, 'Get full workload name'
  )
  if return_code != 0:
    xpk_print(f'Get full workload name request returned ERROR {return_code}')
    return return_code
  full_workload_name = return_value.split(' ')[0]

  # Call kubectl wait on the workload using the full workload name
  timeout_val = args.timeout if args.timeout is not None else -1
  timeout_msg = (
      f'{timeout_val}s' if timeout_val != -1 else 'max timeout (1 week)'
  )
  wait_cmd = (
      "kubectl  wait --for jsonpath='.status.conditions[-1].type'=Finished"
      f' workload {full_workload_name} --timeout={timeout_val}s'
  )
  return_code, return_value = run_command_for_value(
      wait_cmd,
      f'Wait for workload to finish with timeout of {timeout_msg}',
      print_timer=True,
  )
  if return_code != 0:
    if 'timed out' in return_value:
      xpk_print(
          f'Timed out waiting for your workload after {timeout_msg}, see your'
          ' workload here:'
          # pylint: disable=line-too-long
          f' https://console.cloud.google.com/kubernetes/service/{get_cluster_location(args.project, args.cluster, args.zone)}/{args.cluster}/default/{args.workload}/details?project={args.project}'
      )
      return 124
    else:
      xpk_print(f'{return_value}')
      xpk_print(f'Wait for workload returned ERROR {return_code}')
      return return_code
  xpk_print(
      'Finished waiting for your workload, see your workload here:'
      # pylint: disable=line-too-long
      f' https://console.cloud.google.com/kubernetes/service/{get_cluster_location(args.project, args.cluster, args.zone)}/{args.cluster}/default/{args.workload}/details?project={args.project}'
  )
  status_cmd = (
      f'kubectl get jobset {args.workload} -o'
      " jsonpath='{.status.conditions[-1].type}'"
  )
  return_code, return_value = run_command_for_value(
      status_cmd, 'Get jobset status'
  )
  if return_code != 0:
    xpk_print(f'Get workload status request returned ERROR {return_code}')
    return return_code
  xpk_print(f'Your workload finished with status: {return_value}')
  if return_value != 'Completed':
    xpk_print('Your workload did not complete successfully')
    return 125
  return 0


GCP_NAME_FILTER_VALUE_REGEX = re.compile(r'[a-z0-9\-]+')
"""Defines correct name prefix value (contains only letters, numbers and dashes) that can be used in GCP filter chips."""


def get_jobsets_list_gcp_link(project: str) -> str:
  """Returns a link to Cloud Console JobSets list"""

  return f'https://console.cloud.google.com/kubernetes/aiml/deployments/jobs?project={project}'
