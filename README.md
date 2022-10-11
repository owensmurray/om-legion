# om-legion

Legion is framework for building clusters of homogenous nodes that must
maintain a shared, replicated state.

For instance, maybe in order to scale a service you are using a partitioning
strategy to direct traffic for users A-H to node 1, users I-Q to node 2, and
R-Z to node 3. The partition table itself is something all the nodes have to
agree on and would make an excellent candidate for the "shared, replicated
state" managed by this framework.

To update the shared state, whatever it is, use:

* `applyFast`
* or `applyConsistent`

Additionally, since Legion already has to understand about different nodes in
the cluster, and how to talk to them, we provide a variety of tools to help
facilitate inter-node communication:

* `cast`: We support sending arbitrary messages to other peers without
  waiting on a response.

* `call`: We support sending requests to other peers that block on a
  response.

* `broadcast`: We support sending a message to _every_ other peer without
  waiting on a response

* `broadcall`: We support sending a request to every other peer, and blocking
  until we recieve a response from them all.


