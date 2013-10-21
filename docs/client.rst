:mod:`flowy.client`
-------------------

.. automodule:: flowy.client


.. autoclass:: SWFClient
   :members:

.. autoclass:: Decision
   :members:

   .. attribute:: name

     The name of the workflow type defined together with
     :attr:`flowy.client.Decision.version`.

   .. attribute:: version

     The version of the workflow type defined together with
     :attr:`flowy.client.Decision.name`.

.. autoclass:: ActivityClient
   :members:

.. autoclass:: ActivityResponse
   :members:

   .. attribute:: name

    The name of the activity type defined together with
    :attr:`ActivityResponse.version`.

   .. attribute:: version

    The version of the activity type defined together with
    :attr:`ActivityResponse.name`.

   .. attribute:: input

    The input provided when the activity task was scheduled.
