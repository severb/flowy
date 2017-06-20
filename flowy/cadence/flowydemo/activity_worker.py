import tchannel.sync

import flowy.cadence.client
import flowy.cadence.config
import flowy.cadence.worker

import activities


config = flowy.cadence.config.SWFActivityConfig()

activity_worker = flowy.cadence.worker.SWFActivityWorker()

# This is explicit registration.
# There's a better way to do it with decorators
# but just want to keep things clear for this demo.

activity_worker.register(
    config,
    activities.sum,
    version=1,
)


activity_worker.register(
    config,
    activities.mul,
    version=1,
)


activity_worker.register(
    config,
    activities.err,
    version=1
)


tchannel = tchannel.sync.TChannel('flowy')
c = flowy.cadence.client.SWFClient(tchannel)
activity_worker.run_forever('demo', 'demotl', c)
