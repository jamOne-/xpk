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
  assert 'Priority:.spec.podSets[0].template.spec.priorityClassName' in command
