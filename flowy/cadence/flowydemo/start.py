import sys

import tchannel.sync

import flowy.cadence.starter
import flowy.cadence.client


workflow_name = sys.argv[1]
args = map(int, sys.argv[2:])


tchannel = tchannel.sync.TChannel('flowy')
c = flowy.cadence.client.SWFClient(tchannel)

print flowy.cadence.starter.SWFWorkflowStarter(
    domain='demo',
    name=workflow_name,
    version=1,
    swf_client=c,
    task_list='demotl',
    task_duration=10,
    workflow_duration=120,
)(*args)
