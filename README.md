# README #

This is an implementation of the chord protocol which is based on the landmark paper (https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf), which is a P2P protocol  in Scala, using the Akka actor framework to simulate peers in the network. We implemented a simple file location and transfer service based on this protocol.

What's awesome about this protocol is that it is backed by robust mathematics which proves that any peer can obtain resources from any other peer in O(logn) hops. It also uses a distributed hashing algorithm to balance the load among the peers in the network which is a good property to have in any P2P system.  


### How do I get set up? ###

This is an sbt-based Scala project so make sure you have both sbt and scala installed and simply run the program by specifying the number of nodes you wish to create in the p2p network simulation.
