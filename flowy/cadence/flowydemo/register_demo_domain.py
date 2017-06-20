from flowy.cadence.cadence import cadence_frontend, shared
from tchannel.sync import TChannel

t = TChannel('flowy')

print t.thrift(
    cadence_frontend.WorkflowService.RegisterDomain(
        shared.RegisterDomainRequest(
            name='demo'
        )
    )
).result().body
