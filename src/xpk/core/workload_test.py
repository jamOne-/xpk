"""
Copyright 2025 Google LLC

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

from unittest.mock import MagicMock, patch
from xpk.core.workload import get_jobsets_list_gcp_link, get_workload_list


def test_get_jobsets_list_gcp_link():
  result = get_jobsets_list_gcp_link(
      project='test-project',
  )

  assert (
      result
      == 'https://console.cloud.google.com/kubernetes/aiml/deployments/jobs?project=test-project'
  )


@patch('xpk.core.workload.run_command_for_value')
def test_get_workload_list(mock_run_command):
  # Setup
  mock_run_command.return_value = (0, 'Jobset Name...')
  args = MagicMock()
  args.filter_by_status = 'EVERYTHING'
  args.filter_by_job = None

  # Execute
  get_workload_list(args)

  # Verify
  call_args = mock_run_command.call_args
  command = call_args[0][0]

  # Assert
  assert 'jsonpath=' in command
  assert '{.spec.podSets[*].count}' in command


@patch('xpk.core.workload.run_command_for_value')
def test_get_workload_list_super_slicing(mock_run_command):
  # Setup
  # Mock output with super-slicing (multiple counts) and normal workload
  mock_output = (
      'job-super~2024-01-01T00:00:00Z~high~32 32~32 32~0 0~Running~All'
      ' good~2024-01-01T00:01:00Z\njob-normal~2024-01-02T00:00:00Z~low~4~4~0~Running~All'
      ' good~2024-01-02T00:01:00Z\njob-pending~2024-01-03T00:00:00Z~high~16~~0~Admitted~Waiting~2024-01-03T00:01:00Z'
  )
  mock_run_command.return_value = (0, mock_output)
  args = MagicMock()
  args.filter_by_status = 'EVERYTHING'
  args.filter_by_job = None

  # Execute
  return_code, return_value = get_workload_list(args)

  # Verify
  assert return_code == 0
  # Check for summed values
  assert 'job-super' in return_value
  assert '64' in return_value  # 32 + 32 Needed
  assert '64' in return_value  # 32 + 32 Running
  assert '0' in return_value  # 0 + 0 Done
  assert 'job-normal' in return_value
  assert '4' in return_value
  assert 'job-pending' in return_value
  assert '<none>' in return_value  # Running for job-pending
